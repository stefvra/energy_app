# Energy app


## Introduction

This project is a system of components that can be used to monitor and control electric consumption. It is targetted to run on a [Raspberry Pi](https://www.raspberrypi.org/) and read out the P1 port of the smart electricity meter as used in Belgium. It can combine consumption readings with solar panel data (for example see bottom section).

It originates from my personal journey into learning programming. As a learning I try to use a wide variety of technologies and design patterns (See section below). Therefore this project can be usefull for programmers trying to learn more on certain topics as well as for people who want to get more insight in the electricity consumption. My current setup is to monitor the data via a web app and control the charging of an electric vehicle. It is being developed in [Visual Studio Code] (https://code.visualstudio.com/).


## Architecture

The system consists of a number of flexible components that can be configured via config files. An example of a config file can be found [here](/config/general/test.conf). This is also the config file that is used for testing purposes. There are two main types of components: apps and the web app. Apps are applications or services that run in the background and perform several actions like logging, controlling, managing data... The web app is a Flask application that can be used to interact with the system. A data store is needed. For the moment 2 types are foreseen: csv files and Mongodb. The web app as well as the apps can be run via a process control system like [Supervisor](http://supervisord.org/). Besides the web app and apps there are endpoints that provide access to external components (data stores, serial port...)


### Web app

The web app is a [Flask](https://flask.palletsprojects.com/en/2.0.x/) application that provides an way to interact with the system. The webpage can be built up in a flexible way by setting the config file. Below an example screenshot.

![title](/images/webapp_screenshot.png)

Possible components are:

* Realtime data: provides the last recorded data. Consumed electricity and solar power

![title](/images/realtime_screenshot.png)


* Daily graph: provides a visual graph for the consumed electricity and the solar power over a selected day. The requested date can be provided as query parameter 'date, yyyy-mm-dd' in the http request

![title](/images/daily_graph_screenshot.png)


* Date selection buttons: these buttons request for today, previous day or next day

![title](/images/date_selection_screenshot.png)


* Daily totals: shows labels with the daily totals. Info provided:
    * Electricity consumed: electricity consumed from the grid
    * Electricity returned: electricity returned to the grid
    * Direct consumption: electricity consumed directly from PV installation
    * Solar energy: electricity generated from PV installation
    * Cost: estimated electricity cost
    * Electricity consumed: estimated profit from PV installation (= cost witout PV installation - actual cost)

![title](/images/daily_totals_screenshot.png)


### Backend Apps

The backend apps are a set of services that run in the background to manage the data. They run with fixed cycle times. 

* Loggers: services that read in data from a reader and write it in a store. These are the main apps to input the data in the stores

* Data Managers: services that manipulate data in the store. They read in data from a store, manipulate it and write it back into another store. For example to compress data, get daily totals...

* Controllers: services that read data from a store and steer an output based upon a control strategy. For example control a relay to charge an electric vehicle.



### Endpoints

Endpoints are components that manage the interface with external systems. Important to not is that data is handled in [Pandas](https://pandas.pydata.org/) Dataframe format so a big part of the endpoints job is to convert to/from DataFrames.

* agent_clients: the services in the backend apps have internal configuration and states. If the service (or agent) is configured to be requestable, it can be accessed via websockets to get states and configuration as wel as to change configuration. This component manages the client side of this communication. 

* outputs: component that sets Raspberry Pi digital outputs. These can be used in controllers

* readers: components that read in external data

    * PV readers: reads from photo voltaic installation (currently only fronius reader installed)

    * DSMR reader: readr from P1 port in DSMR smart meter. reader is wrapper for ndokters [dsmr_parser] (https://github.com/ndokter/dsmr_parser)

    * OW reader: reader for meteo data from [openweathermap] (https://openweathermap.org/). API subscription is needed

    * MCP3008 reader: reader for analog signals via the MCP3008 IC. As the Raspberry Pi has no analog inputs this chipset can be used to read in analog signals and send it to the RPI via SPI bus.

* stores: these are endpoints to datastores. They all convert dataframes to and from the store with as index the timestamp. All times are localized. Current supported stores are CSV store and Mongodb store (can be on Raspberry Pi or on other device). Multiple configurations are possible via the decorator pattern (see below):

    * Lazy loading: will check if same get request has ben done before. If so and within timeframe it will return previous result that is stored in memory.  This can be used to improve response time for data that is not time critical

    * Distributed: distributes the data over different files/collections according to distribution strategy. If the distribution strategy is executed in the time index this can be used to speed up search

    * No duplicates: idea is to guarantee that no duplicate data is in the store. Currently not tested

### Tools

The tools are a few base components that are used troughout the system. 


## Technologies and patterns used

As I use this program to learn more about programming I try to use a wide variety of technologies. Therefore they might not always be the optimal choice.

### Testing

Most of the code is tested via the [Pytest] (https://docs.pytest.org/en/7.0.x/) library. Test code is included in the component directory itself under the test directory. Comments about the individual tests are foreseen inline. The testplan is being executed with the VSC test explorer.

### Logging

Logging is added here and there in the system. The logging is based on the python logging module. The loggers are configured in a config [file (config/logging/production.conf)].


### Emulation

For testing purposes emulation is needed. The raw emulation code can be found [here] (endpoints/tests/emulators.py).

* For PV reader: as the PV server is not always available in the test environment, some tests are run with an emulated PV server. This server is implemented with the pytest http server library.
* For testing the DSMR reader a serial connection is needed. This is emulated by:
    * Opening a socat service between server and client device. Socat is a Linux bidirectional pipe handler
    * Starting a suprocess that feeds the server side of the pipe with emulated DSMR data


### Websockets

For requestable services a websocket connection is opened. This connection can be accessed (ports need to be opened on the host) by clients. On the server side asynchronous websockets are used.


### Asynchronous programming

This technique is used mainly for the backend applications (loggers, data managers...). As they need to run on fixed timing (for example logging every 5 seconds) and do not always have fixed execution times (for example due to http requests) this technique cannot be avoided. All the services have a periodic event that triggers them to execute.


### Model View Controller

The web app is built up as a model view controller. Different parts are seperated in different files:

* Controller: there is only one controller for the dashboard. This controller is quite basic as little url routing needs to be done

* View: the view is handled in html templating. Templates are available in the templates directory

* Model: this is the biggest part of the webapp. In the model, the needed data is fetched and processing is execture in order to send the right data to the view
 

### Decorator pattern

The store endpoints are buildup around the decorator pattern. Altough this was not a nessesity, it shows a nice usage of this pattern. The idea is that a store client can be decorated as wanted with multiple functionalities: distributed, lazy, non duplicate... This enables any combination of these functionalities.


### Strategy pattern

The strategy pattern is used troughout the different components. One specific usage is in the backend services. Here the agent class is representing the service. As the actual logic of the service can be anything, the logic is extracted in a strategy class. This class has a number of attributes like configurations and states. It also has an execute method that runs the strategy based on a number of inputs and commands that are passed as arguments.


### Factory pattern

The factory pattern is used all over teh system. As the apps consists out of multiple classes the factories take care of instantiating the classes in the correct way. Another functionallity of the factories is to create apps from config files.


### Command pattern

The backend services use the command pattern to execute commandts from within the strategy. This is not a command in a pure sense but as the execution of the commands is virtualized in a command class this can be seen as the command pattern.


### Template pattern

The template pattern is also used troughout the system. It uses a method from a base class as template and fills in the blanks with methods of child classes. The reader read method is an example (altough not usefull as the template is just calling the child method). Here the read method relies on the _read method, that is implemented in the child classes.


### State pattern

The state pattern is used in the strategy of the data managers. This strategy relies on a block object that represents a time interval in which an algorithm needs to be executed. This block object can have different states and based upon the states the algorithm needs to be executed or not. Therefore an ideal application of the state pattern. The states of the blocks are:

* Todo: block needs to be processed
* Faulty: processing has been tried but error occured in execution of algorithm
* Done: block is comming out of succesful processing
* Closed: block has been in done state and has been processed at least one time after this


 ```mermaid
flowchart TD
    A(TODO) --> |error during processing| B
    A --> |processed| C
    B(FAULTY) --> |error during processing| B
    B --> |processed| C
    C(DONE) --> |automatically| E
    E(CLOSED)
 ```




## Example

## Future Work

As future work I see:
 * Smart controller: as I have a lot of data from the last years it is possible to make the controller a lot smarter. It could predict the solar power based on the weather predictions as well as the consumption. This can enable a scheduling algorithm for the controller.
 * Output for smart plugs
 * Reader for other PV installations like SMA sunnyboy
 * Expand and improve logging troughout the components
 * Implement propper exception handling at all levels


