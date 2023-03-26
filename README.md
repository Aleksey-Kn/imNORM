# ImNORM
![Lines of code](https://img.shields.io/tokei/lines/github/Aleksey-Kn/imNORM)
[![Java CI](https://github.com/Aleksey-Kn/imNORM/actions/workflows/build.yml/badge.svg)](https://github.com/Aleksey-Kn/imNORM/actions/workflows/build.yml)
[![GitHub](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com//Aleksey-Kn/imNORM/blob/master/LICENSE "MIT")

In-memory NoSQL ORM for Java, worked on file system. 

## Requirements
1. Java 15 or above
2. Maven

## The service provides the following functions:
- Thread-safe CRUD functional, working on key-value data storage
- Ability to control the memory allocated for repositories
- Transactionality that ensures data integrity
- The possibility of obtaining records by condition

## Building
```
git clone https://github.com/Aleksey-Kn/imNORM.git
cd imNORM
mvn install
```

## Download with Maven from GtHub packages
```
<dependency>
    <groupId>io.github.alekseykn</groupId>
    <artifactId>imnorm</artifactId>
    <version>1.1.0</version>
</dependency>
```

Added in settings.xml (/user/.m2)
```
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/aleksey-kn/imnorm</url>
    </repository>
</repositories>
```

## Issue tracking
Found a bug? Have an idea for an improvement? Feel free to [file an issue.](https://github.com/Aleksey-Kn/imNORM/issues)
