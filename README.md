# kafkaDynamics
A Kafka with DynamoDB Sink Connector in C

A high-performance microservice written in pure C 
that consumes event data from an **Apache Kafka** topic, validates incoming payload schemas, and prepares update records for **Amazon DynamoDB**.
Features a built-in **Dead Letter Queue (DLQ)** handler that automatically intercepts invalid JSON strings or corrupt schemas and splits them off to a fallback Kafka topic for diagnostics.

## Prerequisites

Ensure you have a C99 compatible compiler toolchain (`gcc` or `clang`), `cmake`, and the following libraries installed on your development environment:

* **librdkafka** (Apache Kafka C Client Library)
* **cJSON** (Ultra-lightweight JSON parser for C)

### Installation on Ubuntu / Debian:
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libtool pkg-config librdkafka-dev libcjson-dev
```

## Compilation

Build the project using standard CMake workflows:

```bash
# Create and navigate to build directory
mkdir build && cd build

# Generate build configuration files
cmake ..

# Compile the executable binary
make
```

This compiles a native execution file named `kafka_sink`.

## Configuration & Usage

1. Open `main.c` to modify broker infrastructure and topic strings if needed:
   * `KAFKA_BROKERS`: Endpoint of your Kafka broker setup (default: `localhost:9092`).
   * `SOURCE_TOPIC`: Incoming events topic (default: `user-updates`).
   * `DLQ_TOPIC`: Fallback target for serialization or processing errors (default: `user-updates-dlq`).

2. Execute the compiled connector:
   ```bash
   ./kafka_sink
   ```

## Expected Payload Schema

The application looks specifically for `flat` payloads fitting the following schema definition. Any malformed structure will instantly trigger a DLQ transfer.

```json
{
  "user_id": "usr_98745",
  "status": "active",
  "login_count": 1
}
```
## Multiplatform Support
To make the C file fully multiplatform: to support Multi-platform OSs ( Linux, macOS, and Windows via `MSVC`),
we need to address a few cross-platform C development hurdles:
1. Header differences: Windows uses `<windows.h>` header file
   (does not feature standard POSIX keywords like `pthread` or `standard error` structures natively in the same way,
2. `librdkafka`: however, it thankfully abstracts the networking layers internally.
3. *Infinite Loops & Signals*: Using an absolute `while(1)` block can become a `runaway` background process on Windows if not handled cleanly: Hence we'd replace it with a `volatile` flag controlled by a standard signal handler.
4. *Cross-Platform String/Buffer Printing*: Safe string mutations differ across operating systems (e.g., `strcpy_s` on Windows vs `strncpy` on Unix). We will standardize stuff using cross-platform `safe` parameters.

### Compilation via Windows (PowerShell / vcpkg)
```powershell
# Install system packages via vcpkg
vcpkg install librdkafka cjson

# Generate build engine
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --config Release
```

Would you like to expand the build matrix to include a **GitHub Actions workflow pipeline file (`.github/workflows/build.yml`)** to run tests and cross-compile this project automatically across Linux, macOS, and Windows runners?

