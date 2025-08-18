## Bus 
Simple bus for asyncronus communication between microservices. It uses Redis streams for communication.

## Used libraries
* https://github.com/redis/go-redis/ - Redis client for Go (BSD-2-Clause license)
* https://github.com/ra-company/logging - Simple logging library (GPL-3.0 license)
* https://github.com/ra-company/database - Simple Database interface with logging (GPL-3.0 license)
* https://github.com/ra-company/env - Simple environment library (GPL-3.0 license)
* https://github.com/stretchr/testify - Module for tests (MIT License)
* https://github.com/brianvoe/gofakeit/ - Random data generator written in go (MIT License)

# Staying up to date
To update library to the latest version, use go get -u github.com/ra-company/bus.

# Supported go versions
We currently support the most recent major Go versions from 1.25.0 onward.

# License
This project is licensed under the terms of the GPL-3.0 license.