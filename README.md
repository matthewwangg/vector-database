# vector-database
A C++20 vector database engine for high-performance similarity search. It provides HNSW and Flat index implementations, persistence via snapshots and WAL, replication, caching, and metrics — all exposed through a gRPC API.

## 📌 Features
- HNSW and Flat indexes for vector similarity search.
- Persistence via snapshots and write-ahead logging.
- Primary/replica replication with background sync.
- LRU-based query cache for batch workloads.
- Built-in metrics and statistics collection.
- gRPC API for inserts, deletes, searches, and management.

## 📁 Project Structure
```
vector-database
├── engine/                 # Core components and database logic
├── server/                 # gRPC service implementation and entrypoint
├── Dockerfile              # Container build for server
└── CMakeLists.txt          # Build configuration
```

## 🚀 Deployment

### ⚙️ Available Flags
```
--replica                              Run this node as a replica
--no-cache                             Disable query cache
--replica-address <addr>               Add a replica peer address (primary only)
--sync-server-address <addr>           Address of primary for syncing (replica only)
--cleanup-interval <ms>                Background cleanup interval (default 60000)
--sync-interval <ms>                   Replication sync interval (default 90000)
--logger-type <local|remote|silent>    Configure logging backend (default local)
```

### 🐳 Docker
Build the image:
```
docker build -t vector-database .
```

Run a primary server (default port 50051):
```
docker run -p 50051:50051 vector-database \
  ./server primary 0.0.0.0:50051 --replica-address=0.0.0.0:50053
```

Run a replica with sync from primary:
```
docker run -p 50052:50052 vector-database \
  ./server replica1 0.0.0.0:50052 \
  --replica \
  --sync-server-address=0.0.0.0:50053
```

Multi-node example with Docker network:
```
docker network create vec-net
```

Start primary:
```
docker run -it --rm --network=vec-net --name=primary \
  vector-database ./server primary primary:50051 --replica-address=0.0.0.0:50053
```

Start replica:
```
docker run -it --rm --network=vec-net --name=replica1 \
  vector-database ./server replica1 replica1:50052 \
  --replica --sync-server-address=0.0.0.0:50053
```

### 🧪 Run Locally
Build with CMake:
```
mkdir build && cd build
cmake ..
make -j
```

Generate gRPC dependencies with protoc:
```
# Engine protos
protoc --proto_path=engine/proto \
       --cpp_out=engine/proto \
       --grpc_out=engine/proto \
       --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
       --experimental_allow_proto3_optional \
       engine/proto/*.proto

# Server protos
protoc --proto_path=server/proto \
       --cpp_out=server/proto \
       --grpc_out=server/proto \
       --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
       server/proto/*.proto
```

Run a primary server:
```
./server primary 0.0.0.0:50051
```

Run a replica connected to primary:
```
./server replica1 0.0.0.0:50052 \
  --replica \
  --sync-server-address=127.0.0.1:50051
```

## 🌐 API Usage

Generate gRPC stubs from the server protos:
```
protoc --proto_path=server/proto \
       --cpp_out=server/proto \
       --grpc_out=server/proto \
       --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
       server/proto/*.proto
```

Example RPCs:
```
Insert          Add a new vector with optional payload  
Remove          Delete a single vector by ID  
Search          Query nearest neighbors
  
BatchInsert     Insert multiple vectors efficiently  
BatchRemove     Remove multiple vectors
BatchSearch     Query nearest neighbors with multiple queries
  
ListTables      List all available tables  
CreateTable     Create a new vector table with index configuration  
DropTable       Drop an existing table and its data  

Stats           Retrieve table-level statistics 
Metrics         Retrieve table-level metrics
```
