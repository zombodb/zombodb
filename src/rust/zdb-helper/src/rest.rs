use pg_bridge::stringinfo::{StringInfo, ToPostgres};
use pg_bridge::*;
use std::ffi::CStr;
use std::io::Read;
use std::os::raw::c_char;

const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

#[pg_guard]
pub extern "C" fn rest_call(
    method: *mut c_char,
    url: pg_sys::StringInfo,
    post_data: pg_sys::StringInfo,
    compression_level: usize,
) -> pg_sys::StringInfo {
    let method = unsafe { CStr::from_ptr(method).to_string_lossy().to_string() };
    let post_data = StringInfo::from_pg(post_data);
    let url = match StringInfo::from_pg(url) {
        Some(url) => {
            let mut url = url.to_string();

            // libcurl used to assume 'http://' if no scheme was included, but reqwest doesn't
            // like a url without a scheme
            if !url.starts_with("http://") && !url.starts_with("https://") {
                // so fake in http://
                url = format!("http://{}", url).to_string();
            }
            url
        }
        None => error!("url is null"),
    };

    let client = reqwest::ClientBuilder::new()
        .gzip(compression_level > 0)
        .tcp_nodelay()
        .timeout(TIMEOUT)
        .build();

    match client {
        Ok(client) => {
            let mut builder = match method.as_str() {
                "GET" => client.get(url.as_str()),
                "POST" => client.post(url.as_str()),
                "PUT" => client.put(url.as_str()),
                "DELETE" => client.delete(url.as_str()),
                unknown => error!("unrecognized HTTP method: {}", unknown),
            };

            let mut headers = reqwest::header::HeaderMap::new();
            headers.append(
                "Content-Type",
                reqwest::header::HeaderValue::from_static("application/json"),
            );

            if let Some(post_data) = post_data {
                if compression_level > 0 {
                    headers.append(
                        "Content-Encoding",
                        reqwest::header::HeaderValue::from_static("deflate"),
                    );
                    builder = builder.body(miniz_oxide::deflate::compress_to_vec(
                        post_data.to_string().as_bytes(),
                        compression_level as u8,
                    ))
                } else {
                    builder = builder.body(post_data.to_string())
                }
            }

            builder = builder.headers(headers);
            match builder.send() {
                Ok(mut response) => {
                    let status = response.status().as_u16();

                    if status < 200 || (status >= 300 && status != 404) {
                        error!("unexpected http response code from remote server.  code={}, response={:?}", status, response);
                    }

                    let body = &mut String::new();
                    let _size = response.read_to_string(body).unwrap_or(0);
                    body.to_string().to_postgres()
                }
                Err(e) => panic!("{}", e),
            }
        }
        Err(e) => panic!("{}", e),
    }
}
