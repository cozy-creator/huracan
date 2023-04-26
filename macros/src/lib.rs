use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(PulsarMessage)]
pub fn pulsar_message(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = parse_macro_input!(input as DeriveInput);

	let name = input.ident;

	let code = quote! {
		impl pulsar::SerializeMessage for #name {
			fn serialize_message(input: Self) -> anyhow::Result<pulsar::producer::Message, pulsar::Error> {
				let payload = serde_json::to_vec(&input).map_err(|e| pulsar::Error::Custom(e.to_string()))?;
				Ok(producer::Message { payload, ..Default::default() })
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
