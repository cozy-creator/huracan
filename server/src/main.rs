use std::collections::BTreeMap;

use actix_web::{get, guard, post, web, App, HttpRequest, HttpResponse, HttpServer, Result as WebResult};
use async_graphql::{
	extensions::ApolloTracing, http::GraphiQLSource, EmptyMutation, InputObject, Object, Schema, Subscription, ID,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use async_stream::stream;
use futures::Stream;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct SuiIndexedObject {
	pub id:                     String,
	pub version:                u64,
	pub digest:                 String,
	// TODO can this ever be anything different? see variant `Package`
	pub type_:                  SuiIndexedType,
	pub owner:                  Option<String>,
	pub ownership_type:         SuiOwnershipType,
	pub initial_shared_version: Option<u64>,
	pub previous_transaction:   String,
	pub storage_rebate:         Option<u64>,
	pub fields:                 BTreeMap<String, SuiMoveValue>,
	pub bcs:                    Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[serde(untagged)]
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

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub enum SuiOwnershipType {
	Address,
	Object,
	Shared,
	Immutable,
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct SuiIndexedType {
	pub type_:    String,
	pub package:  String,
	pub module:   String,
	pub struct_:  String,
	pub generics: Vec<String>,
}

#[Object]
impl SuiIndexedObject {
	async fn dynamic_fields(&self) -> Vec<SuiIndexedObject> {
		todo!()
	}
}

#[Object]
impl QueryRoot {
	async fn object(&self, id: ID) -> String {
		format!("hello {}", *id)
	}

	async fn objects(&self, _args: ObjectArgsInput) -> Vec<String> {
		vec![format!("hello")]
	}

	// + owners
	async fn owner(&self, address: ID) -> String {
		format!("hello {}", *address)
	}

	// + transactions
	async fn transaction(&self, digest: ID) -> String {
		format!("hello {}", *digest)
	}

	// checkpoint/s?

	// specific methods for coins, nfts, ... any other big object type groups?

	// how to figure out if something is an nft? from package bytecode, see what implements the MVP of fns: mint, transfer, buy, ...?
	// should we index package signatures? so you can also search by those, find all objects touched by any of their fns? or structs or modules
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
	let schema = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
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
