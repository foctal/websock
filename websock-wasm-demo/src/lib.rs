//! Minimal WebAssembly demo for WebSocket + WebSocket-mux in the browser.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

mod echo;
mod echo_mux;

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{Document, HtmlButtonElement, HtmlInputElement};

fn document() -> Document {
    web_sys::window().unwrap().document().unwrap()
}

fn by_id<T: JsCast>(id: &str) -> T {
    document()
        .get_element_by_id(id)
        .unwrap_or_else(|| panic!("missing element: #{id}"))
        .dyn_into()
        .unwrap()
}

fn append_log(msg: &str) {
    let doc = document();
    let el = doc.get_element_by_id("log").unwrap();
    let mut text = el.text_content().unwrap_or_default();
    text.push_str(msg);
    text.push('\n');
    el.set_text_content(Some(&text));

    // Also mirror into DevTools for convenience.
    web_sys::console::log_1(&msg.into());
}

fn get_url() -> String {
    let input: HtmlInputElement = by_id("url");
    let url = input.value();
    if url.trim().is_empty() {
        echo_mux::DEFAULT_MUX_URL.to_string()
    } else {
        url
    }
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();

    // Default URL.
    {
        let input: HtmlInputElement = by_id("url");
        if input.value().trim().is_empty() {
            input.set_value(echo_mux::DEFAULT_MUX_URL);
        }
    }

    let btn_conn: HtmlButtonElement = by_id("btn-conn");
    let btn_split: HtmlButtonElement = by_id("btn-split");
    let btn_mux_bi: HtmlButtonElement = by_id("btn-mux-bi");

    // conn demo
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            let url = get_url();
            spawn_local(async move {
                echo::run_conn_demo(&url, |m| append_log(m)).await;
            });
        });
        btn_conn.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // split demo
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            let url = get_url();
            spawn_local(async move {
                echo::run_split_demo(&url, |m| append_log(m)).await;
            });
        });
        btn_split.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    // mux bi demo
    {
        let cb = Closure::<dyn FnMut()>::new(move || {
            let url = get_url();
            spawn_local(async move {
                echo_mux::run_mux_bi_demo(&url, |m| append_log(m)).await;
            });
        });
        btn_mux_bi.set_onclick(Some(cb.as_ref().unchecked_ref()));
        cb.forget();
    }

    append_log("ready");
}
