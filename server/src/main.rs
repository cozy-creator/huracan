use std::collections::BTreeMap;

use actix_web::{get, guard, post, web, App, HttpRequest, HttpResponse, HttpServer, Result as WebResult};
use async_graphql::{
	http::GraphiQLSource, ComplexObject, Context, EmptyMutation, Enum, InputObject, Object, Schema, SimpleObject,
	Subscription, Union, ID,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use async_stream::stream;
use base64::Engine;
use bson::Bson;
use dotenv::dotenv;
use futures::Stream;
use futures_util::TryStreamExt;
use mongodb::{
	bson::{doc, Document},
	options::{ClientOptions, Compressor, FindOptions, IndexOptions, ServerApi, ServerApiVersion},
	Collection, Database, IndexModel,
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

#[derive(Union, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[serde(untagged)]
#[graphql(name = "MoveValue")]
pub enum SuiMoveValue {
	Number(SuiMoveNumber),
	Bool(SuiMoveBool),
	Address(SuiMoveAddress),
	Vector(SuiMoveVec),
	String(SuiMoveString),
	ID(SuiMoveID),
	Struct(SuiMoveStruct),
	Null(SuiMoveNull),
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveNumber")]
pub struct SuiMoveNumber {
	pub value: u32,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveBool")]
pub struct SuiMoveBool {
	pub value: bool,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveAddress")]
pub struct SuiMoveAddress {
	pub value: String,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveString")]
pub struct SuiMoveString {
	pub value: String,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveID")]
pub struct SuiMoveID {
	pub value: String,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveNull")]
pub struct SuiMoveNull {
	// XXX we don't really want to put any value here, but all union members need to be objects
	//		so we're declaring an option type that will just always be None -> null
	pub value: Option<bool>,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveVec")]
pub struct SuiMoveVec {
	pub value: Vec<SuiMoveValue>,
}

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "MoveStruct")]
pub struct SuiMoveStruct {
	pub type_:  String,
	pub fields: BTreeMap<String, SuiMoveValue>,
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

#[derive(SimpleObject, Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[graphql(name = "DynamicField")]
pub struct SuiDynamicField {
	// all relevant generic object fields
	#[graphql(name = "id")]
	pub _id:                  String,
	pub version:              u64,
	#[graphql(name = "type")]
	pub type_:                String,
	pub digest:               String,
	pub previous_transaction: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub storage_rebate:       Option<u64>,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub bcs:                  Vec<u8>,

	// fields specifically tailored for dynamic fields
	pub key_type:   String,
	pub value_type: String,
	pub key:        SuiMoveValue,
	pub value:      SuiMoveValue,
}

#[ComplexObject]
impl SuiIndexedObject {
	async fn dynamic_fields(
		&self,
		ctx: &Context<'_>,
		limit: Option<usize>,
		skip: Option<usize>,
	) -> Vec<SuiDynamicField> {
		let db: &Database = ctx.data_unchecked();
		let c: Collection<Document> = db.collection("dev_testnet_wrappingtest2");
		c.find(
			doc! {
				"object.owner.ObjectOwner": self._id.clone(),
				"object.type": doc! { "$regex": "^0x2::dynamic_field::Field<"},
			},
			FindOptions::builder().limit(limit.map(|v| v as i64)).skip(skip.map(|v| v as u64)).build(),
		)
		.await
		.unwrap()
		.map_ok(|o| parse(&o))
		.map_ok(|o| {
			let key_type = o.type_.generics[0].clone();
			let value_type = o.type_.generics[1].clone();
			SuiDynamicField {
				_id: o._id,
				version: o.version,
				type_: o.type_.full,
				digest: o.digest,
				previous_transaction: o.previous_transaction,
				storage_rebate: o.storage_rebate,
				bcs: o.bcs,
				key_type,
				value_type,
				key: o.fields.get("name").unwrap().clone(),
				value: o.fields.get("value").unwrap().clone(),
			}
		})
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
		let c: Collection<Document> = db.collection("dev_testnet_wrappingtest2");
		match c
			.find_one(
				doc! {
					"_id": id.to_string(),
				},
				None,
			)
			.await
		{
			Ok(Some(o)) => Ok(Some(parse(&o))),
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

/// Parses the pre-graphql-optimized version of a doc into the GraphQL format.
fn parse(o: &Document) -> SuiIndexedObject {
	// items from top-level document
	let id = o.get_str("_id").unwrap().to_string();
	let version = o.get_i64("version_").unwrap() as u64;
	// from here on we're working with the actual object in "object" field:
	let o = o.get_document("object").unwrap();
	// type
	// split on just the first '<' to separate path from generics, if any
	// then for path, split parts on '::'; for generics, split on ','
	let ty = o.get_str("type").unwrap();
	let mut generics = Vec::new();
	let ty = if let Some((ty, terms)) = ty.split_once('<') {
		let terms = &terms[..terms.len() - 1];
		for term in terms.split(",") {
			generics.push(term.trim_start().to_string());
		}
		ty
	} else {
		ty
	};
	let mut it = ty.split("::");
	let package = it.next().unwrap().to_string();
	let module = it.next().unwrap().to_string();
	let struct_ = it.next().unwrap().to_string();
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
	// fields: only for moveObject-s
	let content = o.get_document("content").unwrap();
	let fields =
		if let Ok("moveObject") = content.get_str("dataType") { parse_fields(content) } else { Default::default() };
	// TODO move bcs into function, so we don't have to allocate + decode base64 unless asked for
	let bcs = {
		let bcs_val = o.get_document("bcs").unwrap().get_str("bcsBytes").unwrap();
		let mut bcs = vec![0u8; base64::decoded_len_estimate(bcs_val.len())];
		base64::engine::general_purpose::STANDARD.decode_slice(bcs_val, &mut bcs).unwrap();
		bcs
	};
	let o = SuiIndexedObject {
		_id: id,
		// FIXME
		version,
		digest: o.get_str("digest").unwrap().to_string(),
		type_: SuiIndexedType { full: ty.to_string(), package, module, struct_, generics },
		owner,
		ownership_type,
		initial_shared_version,
		previous_transaction: o.get_str("previousTransaction").unwrap().to_string(),
		storage_rebate: o.get_str("storageRebate").ok().map(|v| v.parse().unwrap()),
		fields,
		bcs,
	};
	o
}

fn parse_fields(o: &Document) -> BTreeMap<String, SuiMoveValue> {
	o.get_document("fields").unwrap().iter().map(|(k, v)| (k.clone(), parse_value(v))).collect()
}

fn parse_value(v: &Bson) -> SuiMoveValue {
	// if it's an object:
	//	if it has .type and .fields -> struct
	//	if it has .id -> id
	//	else -> panic
	// not sure about address
	// rest just map types
	if let Some(doc) = v.as_document() {
		if doc.contains_key("type") && doc.contains_key("fields") {
			SuiMoveValue::Struct(SuiMoveStruct {
				type_:  doc.get_str("type").unwrap().into(),
				fields: parse_fields(doc),
			})
		} else if doc.contains_key("id") && doc.keys().count() == 1 {
			SuiMoveValue::ID(SuiMoveID { value: doc.get_str("id").unwrap().into() })
		} else {
			panic!("don't know how to parse into move value: {:?}\n\nparent: {:?}", doc, v);
		}
	} else if let Some(value) = v.as_i32() {
		// FIXME conversion
		SuiMoveValue::Number(SuiMoveNumber { value: value as u32 })
	} else if let Some(value) = v.as_i64() {
		// FIXME conversion
		SuiMoveValue::Number(SuiMoveNumber { value: value as u32 })
	} else if let Some(value) = v.as_bool() {
		SuiMoveValue::Bool(SuiMoveBool { value })
	} else if let Some(value) = v.as_str() {
		SuiMoveValue::String(SuiMoveString { value: value.into() })
	} else if let Some(value) = v.as_array() {
		SuiMoveValue::Vector(SuiMoveVec { value: value.iter().map(|v| parse_value(v)).collect() })
	} else if let Some(_) = v.as_null() {
		SuiMoveValue::Null(SuiMoveNull { value: None })
	} else {
		panic!("failed to parse into move value: {:?}", v)
	}
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
	let mongo_collection = std::env::var("APP_MONGO_COLLECTION").unwrap_or("dev_testnet_wrappingtest2".into());

	let db = {
		let mut client_options = ClientOptions::parse(mongo_uri).await?;
		// use zstd compression for messages
		client_options.compressors = Some(vec![Compressor::Zstd { level: None }]);
		client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
		let client = mongodb::Client::with_options(client_options)?;
		let db = client.database(&mongo_db);
		let coll = db.collection::<Document>(&mongo_collection);
		// create index for object.owner.ObjectOwner
		coll.create_index(
			IndexModel::builder()
				.keys(doc! {
					"object.owner.ObjectOwner": 1,
				})
				.options(Some(
					IndexOptions::builder()
						.partial_filter_expression(doc! {"object.owner.ObjectOwner": doc!{"$exists": true}})
						.build(),
				))
				.build(),
			None,
		)
		.await
		.unwrap();
		println!("ensured index exists: object owner");
		// create index for object.type
		coll.create_index(
			IndexModel::builder()
				.keys(doc! {
					"object.type": 1,
				})
				.options(None)
				.build(),
			None,
		)
		.await
		.unwrap();
		println!("ensured index exists: object type");
		db
	};

	let schema = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
		.data(db)
		// TODO activate later or on demand or something, don't need that noise for now
		// .extension(async_graphql::extensions::ApolloTracing)
		.limit_depth(10)
		// 32 is also the default
		.limit_recursive_depth(32)
		.limit_complexity(1000)
		.finish();

	Ok(HttpServer::new(move || {
		App::new()
			.app_data(Data::new(schema.clone()))
			.service(
				web::scope(API_PREFIX)
					.service(index)
					// not sure how to make this configuration line shorter, if at all possible
					// actix-web doesn't seem to go very far in their support for config via attributes
					.service(
						resource("/").guard(guard::Get()).guard(guard::Header("upgrade", "websocket")).to(index_ws),
					),
			)
			.service(index_graphiql)
	})
	.bind(("127.0.0.1", 8000))?
	.run()
	.await?)
}

// graphiql
#[get("/")]
async fn index_graphiql() -> WebResult<HttpResponse> {
	let endpoint = format!("{}/", API_PREFIX);
	Ok(HttpResponse::Ok()
		.content_type("text/html; charset=utf-8")
		.body(GraphiQLSource::build().endpoint(&endpoint).subscription_endpoint(&endpoint).finish()))
}
