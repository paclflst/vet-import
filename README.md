## Vet Import

This is a demo of importing of a sample vet visit information in xlsx format into postgres db

The solution consists of 2 parts:
- python console application to execute needed logic
- postgres db to accomodate the data

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)


### Usage

Start needed services with docker-compose

```shell
docker-compose up -d
```

To execute import with a default xlsx file (which is attached to the root of project):

```shell
docker-compose run vet_import  
```

If you wish to import other files place them to the root folder of the project (which is mounted to conteiner volume) and execute 

```shell
docker-compose run vet_import python vet_import.py your_file_name.xlsx
```

### Database
To connect to database and check the results use 

```shell
psql -U import -h localhost -p 7799 -d vet_import
```
then enter *password*

### Logging
Solution logs can be accessed in *application.log* file