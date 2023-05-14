use std::collections::BTreeMap;

use actix_web::{get, guard, post, web, App, HttpRequest, HttpResponse, HttpServer, Result as WebResult};
use async_graphql::{
	extensions::ApolloTracing, http::GraphiQLSource, ComplexObject, Context, EmptyMutation, Enum, InputObject, Object,
	Schema, SimpleObject, Subscription, ID,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use async_stream::stream;
use dotenv::dotenv;
use futures::Stream;
use futures_util::TryStreamExt;
use mongodb::{
	bson::{doc, Document},
	options::{ClientOptions, Compressor, FindOptions, ServerApi, ServerApiVersion},
	Collection, Database,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use web::{resource, Data};

type RootSchema = Schema<QueryRoot, EmptyMutation, SubscriptionRoot>;

struct QueryRoot;

#[derive(InputObject)]
struct ObjectArgsInput {
	ids:    Option<Vec<ID>>,
	owner:  Option<ID>,
	owners: Option<Vec<ID>>,
	// could be just $package, or $p::$module or $p::$m::$struct or $p::$m::$s<$generics...>
	// we parse them, translate them into access via indexes
	#[graphql(name = "type")]
	type_:  Option<String>,
	types:  Option<Vec<String>>,
	// by prev tx digest? --> actually just use tx toplevel query then
	// TODO pagination, how does relay do it?
	limit:  Option<usize>,
	skip:   Option<usize>,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(complex)]
#[graphql(name = "Object")]
pub struct SuiIndexedObject {
	#[graphql(name = "id")]
	pub _id:                    String,
	pub version:                u64,
	pub digest:                 String,
	// TODO can this ever be anything different? see variant `Package`
	#[serde(rename = "type")]
	#[graphql(name = "type")]
	pub type_:                  SuiIndexedType,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub owner:                  Option<String>,
	pub ownership_type:         SuiOwnershipType,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub initial_shared_version: Option<u64>,
	pub previous_transaction:   String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub storage_rebate:         Option<u64>,
	#[serde(skip_serializing_if = "BTreeMap::is_empty")]
	pub fields:                 BTreeMap<String, SuiMoveValue>,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub bcs:                    Vec<u8>,
}

#[derive(Enum, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[serde(untagged)]
#[graphql(name = "MoveValue")]
pub enum SuiMoveValue {
	Number(u32),
	Bool(bool),
	Address(String),
	Vector(Vec<SuiMoveValue>),
	String(String),
	ID(String),
	Struct(SuiIndexedType),
	Option(Box<Option<SuiMoveValue>>),
}

#[derive(Enum, Debug, Deserialize, Serialize, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "OwnershipType")]
pub enum SuiOwnershipType {
	Address,
	Object,
	Shared,
	Immutable,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "Type")]
pub struct SuiIndexedType {
	pub full:     String,
	pub package:  String,
	pub module:   String,
	#[serde(rename = "struct")]
	#[graphql(name = "struct")]
	pub struct_:  String,
	pub generics: Vec<String>,
}

#[ComplexObject]
impl SuiIndexedObject {
	async fn dynamic_fields(&self, ctx: &Context<'_>, limit: usize, skip: usize) -> Vec<SuiIndexedObject> {
		let db: &Database = ctx.data_unchecked();
		let c: Collection<Document> = db.collection("objects");
		c.find(
			doc! {
				"object.owner.ObjectOwner": self._id.clone(),
				// TODO need to also filter for some dynamic_field portion in the type?
			},
			FindOptions::builder().limit(Some(limit as i64)).skip(Some(skip as u64)).build(),
		)
		.await
		.unwrap()
		.map_ok(|o| parse(&o))
		.try_collect()
		.await
		.unwrap()
	}

	// for convenience
	async fn version_hex(&self) -> String {
		format!("{:#x}", self.version)
	}
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum QueryError {
	#[error("internal DB error: {0}")]
	DbError(String),
}

#[Object]
impl QueryRoot {
	async fn object(&self, ctx: &Context<'_>, id: ID) -> async_graphql::Result<Option<SuiIndexedObject>, QueryError> {
		let db: &Database = ctx.data_unchecked();
		let c: Collection<Document> = db.collection("objects");
		match c
			.find_one(
				doc! {
					"_id": id.to_string(),
				},
				None,
			)
			.await
		{
			Ok(Some(o)) => {
				let o = o.get_document("object").unwrap();
				Ok(Some(parse(o)))
			}
			Ok(None) => Ok(None),
			Err(e) => {
				// TODO handle error variants in detail, don't just pass mongo errors through to user
				Err(QueryError::DbError(format!("{:?}", e)))
			}
		}
	}

	async fn objects(&self, ctx: &Context<'_>, _args: ObjectArgsInput) -> Vec<String> {
		let _db: &Database = ctx.data_unchecked();
		vec![format!("hello")]
	}

	// + owners
	async fn owner(&self, ctx: &Context<'_>, address: ID) -> String {
		let _db: &Database = ctx.data_unchecked();
		format!("hello {}", *address)
	}

	// + transactions
	async fn transaction(&self, ctx: &Context<'_>, digest: ID) -> String {
		let _db: &Database = ctx.data_unchecked();
		format!("hello {}", *digest)
	}

	// checkpoint/s?

	// specific methods for coins, nfts, ... any other big object type groups?

	// how to figure out if something is an nft? from package bytecode, see what implements the MVP of fns: mint, transfer, buy, ...?
	// should we index package signatures? so you can also search by those, find all objects touched by any of their fns? or structs or modules
}

fn parse(o: &Document) -> SuiIndexedObject {
	// type
	let ty = o.get_str("type").unwrap();
	let mut it = ty.split("::");
	let package = it.next().unwrap().to_string();
	let module = it.next().unwrap().to_string();
	let struct_ = it.next().unwrap();
	let mut it = struct_.split("<");
	let mut generics = Vec::new();
	let struct_ = if let Some(s) = it.next() {
		let terms = it.next().unwrap();
		let terms = &terms[..terms.len() - 1];
		for term in terms.split(",") {
			generics.push(term.to_string());
		}
		s
	} else {
		struct_
	};
	let struct_ = struct_.to_string();
	// owner
	let owner = o.get_document("owner").unwrap();
	let (owner, ownership_type, initial_shared_version) = if let Ok(addr) = owner.get_str("AddressOwner") {
		(Some(addr.to_string()), SuiOwnershipType::Address, None)
	} else if let Ok(addr) = owner.get_str("ObjectOwner") {
		(Some(addr.to_string()), SuiOwnershipType::Object, None)
	} else if let Ok(shared) = owner.get_document("Shared") {
		// FIXME
		(None, SuiOwnershipType::Shared, Some(shared.get_i64("initial_shared_version").unwrap() as u64))
	} else {
		(None, SuiOwnershipType::Immutable, None)
	};
	// fields
	let content = o.get_document("content").unwrap();
	let fields = if let Ok("moveObject") = content.get_str("dataType") {
		content
			.get_document("fields")
			.unwrap()
			.iter()
			.map(|(k, v)| {
				// TODO parse v into SuiMoveValue struct
				let v = SuiMoveValue::ID(v.to_string());
				(k.clone(), v)
			})
			.collect()
	} else {
		Default::default()
	};
	let o = SuiIndexedObject {
		_id: o.get_object_id("_id").unwrap().to_string(),
		// FIXME
		version: o.get_i64("version_").unwrap() as u64,
		digest: o.get_str("digest").unwrap().to_string(),
		type_: SuiIndexedType { full: ty.to_string(), package, module, struct_, generics },
		owner,
		ownership_type,
		initial_shared_version,
		previous_transaction: o.get_str("previous_transaction").unwrap().to_string(),
		storage_rebate: o.get_str("storage_rebate").ok().map(|v| v.parse().unwrap()),
		fields,
		bcs: o.get_document("bcs").unwrap().get_binary_generic("bcs_bytes").unwrap().clone(),
	};
	o
}

struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
	async fn object(&self, r#type: String) -> impl Stream<Item = String> {
		stream! {
			yield r#type
		}
	}
}

#[post("/")]
async fn index(schema: Data<RootSchema>, req: GraphQLRequest) -> GraphQLResponse {
	schema.execute(req.into_inner()).await.into()
}

async fn index_ws(schema: Data<RootSchema>, req: HttpRequest, payload: web::Payload) -> WebResult<HttpResponse> {
	GraphQLSubscription::new(Schema::clone(&*schema)).start(&req, payload)
}

const API_PREFIX: &'static str = "/api/v1";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
	dotenv().ok();
	let mongo_uri = std::env::var("APP_MONGO_URI").unwrap();
	let mongo_db = std::env::var("APP_MONGO_DB").unwrap_or("sui".into());

	let db = {
		let mut client_options = ClientOptions::parse(mongo_uri).await?;
		// use zstd compression for messages
		client_options.compressors = Some(vec![Compressor::Zstd { level: None }]);
		client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
		let client = mongodb::Client::with_options(client_options)?;
		client.database(&mongo_db)
	};

	let schema = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
		.data(db)
		.extension(ApolloTracing)
		.limit_depth(10)
		// 32 is also the default
		.limit_recursive_depth(32)
		.limit_complexity(1000)
		.finish();

	Ok(HttpServer::new(move || {
		App::new().app_data(Data::new(schema.clone())).service(
			web::scope(API_PREFIX)
				.service(index)
				.service(index_graphiql)
				// not sure how to make this configuration line shorter, if at all possible
				// actix-web doesn't seem to go very far in their support for config via attributes
				.service(resource("/").guard(guard::Get()).guard(guard::Header("upgrade", "websocket")).to(index_ws)),
		)
	})
	.bind(("127.0.0.1", 8000))?
	.run()
	.await?)
}

// graphiql
#[get("/")]
async fn index_graphiql() -> WebResult<HttpResponse> {
	Ok(HttpResponse::Ok()
		.content_type("text/html; charset=utf-8")
		.body(GraphiQLSource::build().endpoint(API_PREFIX).subscription_endpoint(API_PREFIX).finish()))
}
