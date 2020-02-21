# dslink-java-etsdb

[![Build Status](https://drone.io/github.com/IOT-DSA/dslink-java-etsdb/status.png)](https://drone.io/github.com/IOT-DSA/dslink-java-etsdb/latest)

A historian DSLink using ETSDB as the backend driver.

## Distributions

Distributions can be ran independent of Gradle and anywhere Java is installed.
Prebuilt distributions can be found [here](https://drone.io/github.com/IOT-DSA/dslink-java-etsdb/files).

### Creating a distribution

Run `./gradlew distZip` from the command line. Distributions will be located
in `build/distributions`.

### Running a distribution

Run `./bin/dslink-java-etsdb -b http://localhost:8080/conn` from the command
line. The link will then be running.

## Test running

A local test run requires a broker to be actively running.

Running: <br />
`./gradlew run -Dexec.args="--broker http://localhost:8080/conn"`

## Watch Group Logging Type Settings

There are four possible values for the `Logging Type` of a watch group:
- `None`: Nothing will be logged
- `Interval`: Logging will happen on an interval (defined by the `Interval` setting)
- `All Data` and `Point Change`: Logging will happen for every update of the path's value
  - The only difference between `All data` and `Point Change` is that `Point Change` will ignore an update if the value is the same as the last recorded value (in this run of the program). Since dglux only sends subscription updates on value changes (and once on the start of the subscription), this means there's not really any difference, aside from maybe some edge cases.
    - Possibly important to note: Restarting the DSLink is not one of those edge cases. Both `All Data` and `Point Change` will record the initial value when the watch is started up, and so both will end up with a duplicate value in the database. 
