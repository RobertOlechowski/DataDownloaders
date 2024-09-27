# Bitcoin Node Data Fetcher (BTC to DB)
Import BTC node data to Database

## Overview
This application is an open-source Python tool designed to fetch data from a full Bitcoin node and store it in a database for further analysis.
It features data caching to object storage and uses Redis for inter-process communication, operating across multiple processes to enhance performance.

## Build & run

### Build docker image
```bash
git clone https://github.com/RobertOlechowski/BTC_to_DB.git
cd BTC_to_DB
# edit config and set credentials for: redis, mimo, database
cd Scripts
# edit repo name in build.sh
./build.sh
```

### Run docker container
```bash
docker run --name btc-to-db --rm -it registry.home.robertolechowski.com/btc-to-db:latest
```

### Docker composer file
You can override config by environment variables 

``` yaml
services:
  BtcToDb:
    image: btc-to-db
    container_name: btc-to-db
    restart: unless-stopped
    environment:
      BTC2DB_redis.host: XXXXX
      BTC2DB_redis.password: XXXXX

      BTC2DB_node.endpoint: XXXXX
      BTC2DB_node.user: XXXXX
      BTC2DB_node.password: XXXXX

      BTC2DB_mimo.endpoint: XXXXX
      BTC2DB_mimo.access_key: XXXXX
      BTC2DB_mimo.secret_key: XXXXX

      BTC2DB_db.host: XXXXX
      BTC2DB_db.username: XXXXX
      BTC2DB_db.password: XXXXX
```

## Other services in stack
Application is using some external services:
 - BTC NODE
 - Redis
 - Database (optional) 
 - Mimo (optional)

## Todo and known bugs:
 - Restart app and reload config base on redis flag
 - Fetch config from redis

## Contact
This project is developed and maintained by **Robert Olechowski**. 
I encourage you to report any issues or bugs you encounter, and I'm always open to any suggestions for improvements. 
Please feel free to reach out via email or submit an issue on GitHub if you have any questions or need support. 
Your contributions and feedback are highly valued and play a crucial role in the continuous enhancement of this project.

- **Email:** [robertolechowski@gmail.com](mailto:robertolechowski@gmail.com)
- **Website:** [robertolechowski.com](https://robertolechowski.com/)