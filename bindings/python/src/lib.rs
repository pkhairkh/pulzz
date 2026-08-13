#![allow(clippy::all, unsafe_op_in_unsafe_fn, unused_variables, unused_imports)]
//! Python binding for the pulzZ SDK via PyO3.
//!
//! Exposes `pulzz.PulzzClient` plus `CarrierKind`, `SecurityProfile`, `SourceKind` enums.
//!
//! Build:
//!   cd bindings/python && maturin develop --release
//! Use:
//!   import pulzz
//!   client = pulzz.PulzzClient(carrier="websocket", security="pq_simple_v1")

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzClientBuilder, SecurityProfile,
};

fn map_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(format!("{e}"))
}

#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum PyCarrierKind {
    WebSocket,
    Tcp,
    QuicStream,
    QuicDatagram,
    WebTransport,
    UdpDatagram,
}

impl From<PyCarrierKind> for CarrierKind {
    fn from(c: PyCarrierKind) -> Self {
        match c {
            PyCarrierKind::WebSocket => CarrierKind::WebSocket,
            PyCarrierKind::Tcp => CarrierKind::Tcp,
            PyCarrierKind::QuicStream => CarrierKind::QuicStream,
            PyCarrierKind::QuicDatagram => CarrierKind::QuicDatagram,
            PyCarrierKind::WebTransport => CarrierKind::WebTransport,
            PyCarrierKind::UdpDatagram => CarrierKind::UdpDatagram,
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum PySecurityProfile {
    PqMutualV1,
    PqSimpleV1,
    ClassicRef1,
}

impl From<PySecurityProfile> for SecurityProfile {
    fn from(s: PySecurityProfile) -> Self {
        match s {
            PySecurityProfile::PqMutualV1 => SecurityProfile::PqMutualV1,
            PySecurityProfile::PqSimpleV1 => SecurityProfile::PqSimpleV1,
            PySecurityProfile::ClassicRef1 => SecurityProfile::ClassicRef1,
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum PySourceKind {
    Text,
    Json,
    Binary,
    Image,
}

impl From<PySourceKind> for shared_protocol::SourceKind {
    fn from(s: PySourceKind) -> Self {
        match s {
            PySourceKind::Text => shared_protocol::SourceKind::Text,
            PySourceKind::Json => shared_protocol::SourceKind::Json,
            PySourceKind::Binary => shared_protocol::SourceKind::Binary,
            PySourceKind::Image => shared_protocol::SourceKind::Image,
        }
    }
}

fn parse_config_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<ClientConfig> {
    let mut cfg = ClientConfig::default();
    if let Some(c) = dict.get_item("carrier")? {
        if let Ok(s) = c.extract::<String>() {
            cfg.carrier = match s.as_str() {
                "websocket" | "ws" => CarrierKind::WebSocket,
                "tcp" => CarrierKind::Tcp,
                "quic_stream" | "quic" => CarrierKind::QuicStream,
                "quic_datagram" => CarrierKind::QuicDatagram,
                "webtransport" | "wt" => CarrierKind::WebTransport,
                "udp_datagram" | "udp" => CarrierKind::UdpDatagram,
                _ => return Err(PyValueError::new_err(format!("bad carrier: {s}"))),
            };
        }
    }
    if let Some(s) = dict.get_item("security")? {
        if let Ok(v) = s.extract::<String>() {
            cfg.security = match v.as_str() {
                "pq_mutual_v1" | "pq_mutual" => SecurityProfile::PqMutualV1,
                "pq_simple_v1" | "pq_simple" => SecurityProfile::PqSimpleV1,
                "classic_ref1" | "classic" => SecurityProfile::ClassicRef1,
                _ => return Err(PyValueError::new_err(format!("bad security: {v}"))),
            };
        }
    }
    if let Some(b) = dict.get_item("batch_size")? {
        if let Ok(n) = b.extract::<u64>() {
            cfg.batch_size = if n > 0 { Some(n as usize) } else { None };
        }
    }
    if let Some(z) = dict.get_item("zstd_level")? {
        if let Ok(n) = z.extract::<i32>() {
            cfg.compression.zstd_level = n;
            cfg.compression.enabled = n > 0;
        }
    }
    if let Some(t) = dict.get_item("timeout_ms")? {
        if let Ok(n) = t.extract::<u64>() {
            cfg.timeout = std::time::Duration::from_millis(n);
        }
    }
    Ok(cfg)
}

#[pyclass(name = "PulzzClient")]
struct PyPulzzClient {
    inner: Option<PulzzClient>,
    config: ClientConfig,
}

#[pymethods]
impl PyPulzzClient {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let cfg = match kwargs {
            Some(d) => parse_config_from_dict(d)?,
            None => ClientConfig::default(),
        };
        Ok(Self {
            inner: None,
            config: cfg,
        })
    }

    fn connect(&mut self, url: &str, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(map_err)?;
            let cfg = self.config.clone();
            let client = rt
                .block_on(async move { PulzzClient::connect_with_config(url, cfg).await })
                .map_err(map_err)?;
            self.inner = Some(client);
            Ok(())
        })
    }

    fn send(&mut self, item_id: u64, payload: &[u8], py: Python<'_>) -> PyResult<()> {
        let client = self.inner.as_mut().ok_or_else(|| {
            PyRuntimeError::new_err("client is not connected")
        })?;
        py.allow_threads(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(map_err)?;
            rt.block_on(client.send(pulzz_sdk::ItemId(item_id), payload))
                .map_err(map_err)
        })
    }

    fn send_batch(&mut self, items: &Bound<'_, PyList>, py: Python<'_>) -> PyResult<()> {
        let client = self.inner.as_mut().ok_or_else(|| {
            PyRuntimeError::new_err("client is not connected")
        })?;
        let mut collected: Vec<(pulzz_sdk::ItemId, Vec<u8>)> = Vec::with_capacity(items.len());
        for item in items.iter() {
            let tuple = item.downcast::<PyTuple>()?;
            if tuple.len() != 2 {
                return Err(PyValueError::new_err("each item must be (item_id, payload)"));
            }
            let id: u64 = tuple.get_item(0)?.extract()?;
            let payload_bound: Bound<PyBytes> = tuple.get_item(1)?.downcast_into()?;
            collected.push((pulzz_sdk::ItemId(id), payload_bound.as_bytes().to_vec()));
        }
        // Suppress unused warning for py while still allowing send_batch to release the GIL.
        let _ = py;
        py.allow_threads(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(map_err)?;
            rt.block_on(client.send_batch(collected)).map_err(map_err)
        })
    }

    /// recv(timeout_ms=0) -> Optional[tuple[int, bytes, int]]
    /// Returns (item_id, payload_bytes, record_type) or None on EOF.
    #[pyo3(signature = (timeout_ms=0))]
    fn recv(&mut self, timeout_ms: u64, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let _ = timeout_ms; // TODO: thread through to client.config_mut_timeout
        let client = self.inner.as_mut().ok_or_else(|| {
            PyRuntimeError::new_err("client is not connected")
        })?;
        py.allow_threads(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(map_err)?;
            let result = rt.block_on(client.recv());
            match result.map_err(map_err)? {
                None => Ok(None),
                Some(record) => Python::with_gil(|py| {
                    let payload_obj = PyBytes::new_bound(py, &record.payload).into_py(py);
                    let tuple_obj = PyTuple::new_bound(
                        py,
                        &[
                            record.header.item_id.0.to_object(py),
                            payload_obj,
                            (record.header.record_type as u8).to_object(py),
                        ],
                    );
                    Ok(Some(tuple_obj.into_py(py)))
                }),
            }
        })
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(client) = self.inner.take() {
            py.allow_threads(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(map_err)?;
                rt.block_on(client.close()).map_err(map_err)
            })
        } else {
            Ok(())
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
        py: Python<'_>,
    ) -> PyResult<bool> {
        let _ = self.close(py);
        Ok(false)
    }
}

#[pyfunction]
fn pulzz_version() -> String {
    "pulzZ 0.4.0-sdk (Python via PyO3)".to_string()
}

#[pymodule]
fn pulzz(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCarrierKind>()?;
    m.add_class::<PySecurityProfile>()?;
    m.add_class::<PySourceKind>()?;
    m.add_class::<PyPulzzClient>()?;
    m.add("__version__", "0.4.0")?;
    m.add("version_string", "pulzZ 0.4.0-sdk (Python via PyO3)")?;
    m.add_function(wrap_pyfunction!(pulzz_version, m)?)?;
    Ok(())
}

#[allow(dead_code)]
fn _builder_unused() -> PulzzClientBuilder {
    PulzzClientBuilder::default()
}

#[allow(dead_code)]
fn _compression_unused() -> CompressionConfig {
    CompressionConfig::default()
}
