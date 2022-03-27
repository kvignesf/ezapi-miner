# APIOPS 

APIOPS is an awesome api visualization and automated testing tool built on top of OpenAPI specification with Python, Flask, MongoDB and a lot of love!

## Prerequisites

1. Python 3.7.x or higher
2. MongoDB
3. Enchant - A spelling library developed in C (Enchant)



## Initial Setup

Note: These instructions are for macOS and Linux. 

You may need to open multiple bash/command line/terminal windows to run all the commands listed below.

1. Install virtualenv if not installed:
    
        python -m pip install --user virtualenv

2. Set up a virtualenv:

        python -m virtualenv venv
        source ./venv/bin/activate
        pip install -r REQUIREMENTS.txt

3. Set up the database:

        mongo

4. Start Flask server:
    
        python app.py

    Open your browser and browse to [http://127.0.0.1:5000](http://127.0.0.1:5000). You will see a default homepage

## Design Principal

The entire APIOPS Model has 4 components - 
1. parser
2. scorer 
3. visualiser
4. tester

### parser -
parser is used for parsing the original swagger file. It accepts a swagger json file as an input and store the parsed data into following collections - 

* apiinfo - API information and Details of all the tags
* paths - All Path related Information
* requests - All request related (endpoint, params, ...) information
* responses - All response related (status, body, ...) information

### scorer -
scorer is used to extract root level elements which further can be used to visualise the sankey diagram. Also this module scores these elements based on their occurrence and required status. It stores the information into following collections - 

* elements - All root level (no nested) elements. This collection is designed as list of elements inside the tags (resources)
* scorer - Score of above elements

### visualizer -
This module consider elements collection and prepare a sankey data format which can be further utilized to display the API visualization. It also consider endpoints, tags, response code while preparing the data format. Finally, stores this information into following collections - 

* sankey - Store sanket graph data in the form of nodes and links between nodes.


### tester -
This module generate testcase data for all the endpoints and corresponding response status code. Following files have different functionality - 

1. test_generator.py - The main file for generating testdata
2. payload.py - To generate payload data for a given JSON format
3. payload_format.py - To generate the payload schema of a given JSON format
4. reverseRegex.py - To generate text data matching a particular regex pattern

This modules creates following collections - 

* testcases - List of all testcases and corresponding input data and assertion data
* virtual - Similar to testcases, but only required to data to make an actual HTTP call
* test_result - An empty collection for future purpose

## Other Files

1. config.py - Config file for database connection and storing mechanism
2. main.py - Main file which import all submodules and create APIOPSModel class
3. utils.py - Utility library for the application

## Flow Diagram - 
![alt text](APIOPS.png "APIOPS Model")