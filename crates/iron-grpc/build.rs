fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/iron.proto"], &["proto"])
        .expect("protobuf compiler is required to build iron-grpc");
}
