use quote::quote;
use syn::{parse_macro_input, parse_quote, DeriveInput, ItemFn, Stmt};

#[proc_macro_derive(PulsarMessage)]
pub fn pulsar_message(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = parse_macro_input!(input as DeriveInput);

	let name = input.ident;

	let code = quote! {
		impl pulsar::SerializeMessage for #name {
			fn serialize_message(input: Self) -> anyhow::Result<pulsar::producer::Message, pulsar::Error> {
				let payload = serde_json::to_vec(&input).map_err(|e| pulsar::Error::Custom(e.to_string()))?;
				Ok(pulsar::producer::Message { payload, ..Default::default() })
			}
		}

		impl pulsar::DeserializeMessage for #name {
			type Output = anyhow::Result<Self>;

			fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
				Ok(serde_json::from_slice(&payload.data).map_err(|e| pulsar::Error::Custom(e.to_string()))?)
			}
		}
	};

	proc_macro::TokenStream::from(code)
}

#[proc_macro_attribute]
pub fn with_client_rotation(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let ItemFn { mut attrs, vis, sig, block } = parse_macro_input!(item as ItemFn);

	attrs.push(parse_quote! {
		#[async_recursion::async_recursion]
	});

	let Stmt::Expr(call, _) = block.stmts[0].clone() else { panic!("body of function must be an expression like `some_sui_api_call(... args ...).await`!")};

	let code = quote! {
		#(#attrs)* #vis #sig {
			// for this call, we can stay on the current client for as long as it works well for us
			// ("works well": for now this just means not giving us a 429 error, but in the future we may
			// want to track response times (adjusted for # of results or response size returned, potentially))

			// if there's backoff active for the current client, even before trying it, we already
			// want to start the rotation
			// before attempting to find the next best client, we want to sort them by when the next request
			// is allowed, according to their active backoff, if any
			// clients without an active backoff come first
			// use stable sort

			// if we did not find a suitable client to rotate to, we want to:
			// 1) spawn the next client, if any left, and use that
			// 2) select the client whose backoff interval is expiring the soonest

			let client = &mut self.clients[0];
			let api = client.read_api();
			let res = api.#call;
			client.reqs += 1;
			let limited = if let Err(sui_sdk::error::Error::RpcError(jsonrpsee::core::Error::Transport(err))) = res.as_ref() && format!("{}", err).contains("429") {
				true
			} else {
				false
			};
			if !limited {
				// client is OK, so reset backoff, if any
				client.backoff = None;
				return res
			}

			// we were limited, execute backoff handling + client rotation!
			// increment backoff
			{
				let f = client.backoff.map(|b| b.1).unwrap_or(0);
				let backoff_millis = (2u64.pow(f as u32) * 250).min(10_000);
				info!("hit rate limit at {}; backing off for {}ms (factor {})", self.configs[client.id].name, backoff_millis, f);
				client.backoff = Some((Instant::now() + Duration::from_millis(backoff_millis), f + 1));
			}

			// rotate client priority through sorting by backoff timer (first available client at ix 0)
			self.clients.sort_by_key(|c| c.backoff.map(|(t, _)| t));

			// all clients in backoff?
			if let Some((wait_until, _)) = self.clients[0].backoff {
				// any more clients we can create?
				let spawned_new_client = if self.configs.len() > self.clients.len() {
					// new client from next url, set as first to use
					let ix = self.clients.len();
					match self.make_client(ix).await {
						Ok(client) => {
							self.clients.insert(0, client);
							true
						}
						Err(err) => {
							warn!(
								"failed to create additional sui client '{}': {} -- {} -- will try again later!",
								ix, self.configs[ix].name, err
							);
							false
						}
					}
				} else {
					false
				};
				if !spawned_new_client {
					// put backoff into practice: sleep if not past wait time
					let sleep_for = wait_until - Instant::now();
					if sleep_for.as_millis() > 0 {
						tokio::time::sleep(sleep_for).await;
					}
				}
				return self.#call;
			}
			res
		}
	};

	proc_macro::TokenStream::from(code)
}
