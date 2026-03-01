use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::io::Cursor;
use websock_mux_proto::{Frame, StreamDir, StreamId, VarInt};

fn bench_varint_encode(c: &mut Criterion) {
    let values: [u64; 6] = [0, 63, 64, 16_383, 16_384, (1u64 << 62) - 1];
    let mut group = c.benchmark_group("varint_encode");

    for &v in &values {
        group.bench_with_input(BenchmarkId::from_parameter(v), &v, |b, &v| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(8);
                VarInt::from_u64(v)
                    .expect("valid varint value")
                    .encode(&mut buf);
                black_box(buf);
            });
        });
    }

    group.finish();
}

fn bench_varint_decode(c: &mut Criterion) {
    let values: [u64; 6] = [0, 63, 64, 16_383, 16_384, (1u64 << 62) - 1];
    let mut group = c.benchmark_group("varint_decode");

    for &v in &values {
        let mut encoded = BytesMut::with_capacity(8);
        VarInt::from_u64(v)
            .expect("valid varint value")
            .encode(&mut encoded);
        let encoded = encoded.freeze();

        group.bench_with_input(BenchmarkId::from_parameter(v), &encoded, |b, encoded| {
            b.iter(|| {
                let mut cur = Cursor::new(encoded.clone());
                black_box(VarInt::decode(&mut cur).expect("decode succeeds"));
            });
        });
    }

    group.finish();
}

fn bench_frame_encode(c: &mut Criterion) {
    let id = StreamId::new(7, false, StreamDir::Bi).expect("stream id");
    let small = Frame::Stream {
        id,
        data: Bytes::from_static(b"hello"),
        fin: false,
    };
    let large = Frame::Stream {
        id,
        data: Bytes::from(vec![7u8; 64 * 1024]),
        fin: true,
    };

    let mut group = c.benchmark_group("frame_encode");
    group.throughput(Throughput::Bytes(small.encoded_len() as u64));
    group.bench_function("stream_small", |b| {
        b.iter(|| {
            black_box(small.encode());
        });
    });

    group.throughput(Throughput::Bytes(large.encoded_len() as u64));
    group.bench_function("stream_large_64k", |b| {
        b.iter(|| {
            black_box(large.encode());
        });
    });
    group.finish();
}

fn bench_frame_decode(c: &mut Criterion) {
    let id = StreamId::new(7, false, StreamDir::Bi).expect("stream id");
    let small = Frame::Stream {
        id,
        data: Bytes::from_static(b"hello"),
        fin: false,
    }
    .encode()
    .freeze();

    let large = Frame::Stream {
        id,
        data: Bytes::from(vec![7u8; 64 * 1024]),
        fin: true,
    }
    .encode()
    .freeze();

    let mut group = c.benchmark_group("frame_decode");
    group.throughput(Throughput::Bytes(small.len() as u64));
    group.bench_function("stream_small", |b| {
        b.iter(|| {
            let mut cur = Cursor::new(small.clone());
            black_box(Frame::decode(&mut cur).expect("decode succeeds"));
        });
    });

    group.throughput(Throughput::Bytes(large.len() as u64));
    group.bench_function("stream_large_64k", |b| {
        b.iter(|| {
            let mut cur = Cursor::new(large.clone());
            black_box(Frame::decode(&mut cur).expect("decode succeeds"));
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_varint_encode,
    bench_varint_decode,
    bench_frame_encode,
    bench_frame_decode,
);
criterion_main!(benches);
