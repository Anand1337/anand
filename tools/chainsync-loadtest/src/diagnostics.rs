use near_jsonrpc_primitives::types::network_info::{RpcNetworkInfoResponse};
use near_jsonrpc_primitives::types::validator::RpcValidatorResponse;
use near_jsonrpc_primitives::message;
use crate::concurrency::{Ctx};
use anyhow::{anyhow};

pub async fn fetch_network_info(ctx :&Ctx, addr: std::net::SocketAddr) -> anyhow::Result<RpcNetworkInfoResponse> {
    let uri = hyper::Uri::builder()
        .scheme("http")
        .authority(addr.to_string())
        .path_and_query("/")
        .build()?;
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "network_info",
        "params": [],
    });
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .header("Content-Type","application/json")
        .uri(uri)
        .body(hyper::Body::from(serde_json::to_string(&req)?))?;
    
    let resp = ctx.wrap(hyper::Client::new().request(req)).await??;
    let resp = ctx.wrap(hyper::body::to_bytes(resp)).await??;
    //info!("resp = {}",std::str::from_utf8(&resp).unwrap());
    let resp = serde_json::from_slice::<message::Response>(&*resp)?;
    let resp = match resp.result {
        Ok(resp) => resp,
        Err(err) => { return Err(anyhow!(serde_json::to_string(&err)?)) }
    };
    return Ok(serde_json::from_value::<RpcNetworkInfoResponse>(resp)?);
}

pub async fn fetch_validators(uri: hyper::Uri) -> anyhow::Result<RpcValidatorResponse> {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "validators",
        "id": "dontcare",
        "params": [null],
    });
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .header("Content-Type","application/json")
        .uri(uri)
        .body(hyper::Body::from(serde_json::to_string(&req)?))?;
    
    let resp = hyper::Client::new().request(req).await?;
    let resp = hyper::body::to_bytes(resp).await?;
    let resp = serde_json::from_slice::<message::Response>(&*resp)?;
    let resp = match resp.result {
        Ok(resp) => resp,
        Err(err) => { return Err(anyhow!(serde_json::to_string(&err)?)) }
    };
    return Ok(serde_json::from_value::<RpcValidatorResponse>(resp)?);
}
