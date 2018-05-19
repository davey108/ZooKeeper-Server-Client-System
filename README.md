# ZooKeeper Server Client 
This project ultilize Apache ZooKeeper to set up a fault-tolerance servers and clients system where the server will be able to handle failure and still able to complete the clients' transactions safely

## Getting Started
# On Eclipse
1. Download this project and unzip
2. Import the project as a maven project into eclipse
3. Go down to: /src/main/java/edu/gmu/cs475/internal/
4. Run <code>Main.java</code> and follow command prompts

# On Command Line
1. Download this project and unzip
2. On the command line, change directory <code>cd</code> into the folder that this file was unzipped to and run <br>
<code>~/apache-maven-3.5.2/bin/mvn</code>
3. After, run <code>mvn package</code>  in the top level directory and then, in the server directory run <br>
<code>java -jar target/kvstore-0.0.1-SNAPSHOT.jar</code>

### Prerequisites
# On Eclipse
1. Nothing...

# On Command Line
1. <a href="https://maven.apache.org/download.cgi">Maven</a>

## Running the tests
# On Eclipse/IDE's
1. Just run the compile button.

# On Command Line
1. <code>mvn test</code> at top level directory

## Built With
* [Maven](https://maven.apache.org/) - Dependency Management
* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) - Source Code
* [Apache ZooKeeper](https://zookeeper.apache.org/) - ZooKeeper Fault Tolerance System that code uses

## Authors

* **Khang Chau Vo** - *Logic and implementation* - [Project](https://github.com/davey108/ZooKeeper-Server-Client-System)
