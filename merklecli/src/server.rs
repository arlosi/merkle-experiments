use axum::{
    Router,
    extract::{Extension, Path},
    http::{StatusCode},
    response::{Html, IntoResponse},
    routing::get,
};
use axum_server::tls_rustls::RustlsConfig;
use merkletree::{MerkleStore, ReadObjectStore, fsstore::FsStore};
use std::{
    net::SocketAddr, path::{self}, sync::Arc, time::Duration
};

struct Data {
    store: MerkleStore<FsStore>,
    server_url: String,
    depth: usize,
    bredth: usize,
}

pub async fn serve(
    store: MerkleStore<FsStore>,
    port: u16,
    cert: &path::Path,
    key: &path::Path,
    params: (usize, usize),
) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let server_url = format!("https://{addr}/");
    let state = Arc::new(Data {
        store,
        server_url: server_url.clone(),
        depth: params.0,
        bredth: params.1,
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/clear", get(clear))
        .route("/config.json", get(config_handler))
        .route("/{a}/{file_name}", get(crate_handler_short))
        .route("/{a}/{b}/{file_name}", get(crate_handler))
        .route("/merkle/tree/{file_name}", get(merkle_handler_tree))
        .route("/merkle/data/{file_name}", get(merkle_handler_data))
        .layer(Extension(state));

    let config = RustlsConfig::from_pem_file(cert, key).await.unwrap();

    let cert = std::path::absolute(cert).unwrap();

    eprintln!("listening on {server_url}");
    eprintln!("Example cargo configuration:");
    println!(
        r#"
[http]
cainfo = "{}"

[source.crates-io]
replace-with = "merkle"

[source.merkle]
registry = "sparse+{server_url}"
"#,
        cert.display()
    );

    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn config_handler(Extension(store): Extension<Arc<Data>>) -> impl IntoResponse {
    let root = store.store.root().await.unwrap();
    let depth = store.depth;
    let bredth = store.bredth;
    format!(r#"
{{
  "dl": "https://static.crates.io/crates",
  "api": "https://crates.io",
  "merkle": {{
    "depth": {depth},
    "bredth": {bredth},
    "root": "{root}"
  }}
}}
"#)
}

async fn root(Extension(store): Extension<Arc<Data>>) -> impl IntoResponse {
    let server_url = &store.server_url;
    Html(format!(
        r#"
<html><body>
  <h1>Index-Serve</h1>
  <p>Server URL: <a href="{url}">{url}</a></p>
  <ul>
    <li><a href="/clear">clear in memory cache</a></li>
  </ul>
</body></html>
    "#,
        url = server_url
    ))
}

async fn clear(Extension(_store): Extension<Arc<Data>>) -> impl IntoResponse {
    // store.store.inner().clear();
    "cleard"
}

async fn merkle_handler(
    Path(hash): Path<String>,
    Extension(store): Extension<Arc<Data>>,
    is_leaf: bool
) -> impl IntoResponse {
    futures_timer::Delay::new(Duration::from_millis(100)).await;

    match store.store.backend().read(&hash, is_leaf).await {
        Ok(Some(data)) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain")],
            data,
        )
            .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Not Found").into_response(),
        Err(err) => {
            tracing::error!("error getting file {}: {:?}", hash, err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
        }
    }
}

async fn merkle_handler_data(
    hash: Path<String>,
    store: Extension<Arc<Data>>,
) -> impl IntoResponse {
    merkle_handler(hash, store, true).await
}

async fn merkle_handler_tree(
    hash: Path<String>,
    store: Extension<Arc<Data>>,
) -> impl IntoResponse {
    merkle_handler(hash, store, false).await
}

async fn crate_handler(
    Path((_a, _b, file_name)): Path<(String, String, String)>,
    Extension(store): Extension<Arc<Data>>,
) -> impl IntoResponse {
    futures_timer::Delay::new(Duration::from_millis(100)).await;
    // look up the file by name (async)
    match store.store.get_file(&file_name).await {
        Ok(Some(data)) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain")],
            data,
        )
            .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Not Found").into_response(),
        Err(err) => {
            tracing::error!("error getting file {}: {:?}", file_name, err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
        }
    }
}

async fn crate_handler_short(
    Path((_a, file_name)): Path<(String, String)>,
    e: Extension<Arc<Data>>,
) -> impl IntoResponse {
    crate_handler(Path((_a, String::new(), file_name)), e).await
}
