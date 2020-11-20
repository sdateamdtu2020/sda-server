const bodyParser = require('body-parser');
const cors = require('cors');
const express = require('express');
const { EnapsoGraphDBClient } = require("@innotrade/enapso-graphdb-client");

// Config
const GRAPHDB_BASE_URL = "http://localhost:7200",
    GRAPHDB_REPOSITORY = "test2",
    GRAPHDB_USERNAME = "test",
    GRAPHDB_PASSWORD = "test",
    GRAPHDB_CONTEXT_TEST = "https://sda-research.ml/graph/climate/";

const DEFAULT_PREFIXES = [
    EnapsoGraphDBClient.PREFIX_OWL,
    EnapsoGraphDBClient.PREFIX_RDF,
    EnapsoGraphDBClient.PREFIX_RDFS,
    EnapsoGraphDBClient.PREFIX_XSD,
    EnapsoGraphDBClient.PREFIX_PROTONS,
    {
        prefix: "test",
        iri: "http://ont.enapso.com/test#",
    }
];

//Create an Endpoint.
let graphDBEndpoint = new EnapsoGraphDBClient.Endpoint({
    baseURL: GRAPHDB_BASE_URL,
    repository: GRAPHDB_REPOSITORY,
    prefixes: DEFAULT_PREFIXES
});

//Authenticate (Optional)
graphDBEndpoint.login(GRAPHDB_USERNAME,GRAPHDB_PASSWORD)
.then((result) => {
    console.log(result);
}).catch((err) => {
    console.log(err);
});

const app = express();
const port = 5000;

app.use(cors());

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/climate/humidity/', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?humidity from <${GRAPHDB_CONTEXT_TEST}> where {?obs a qb:Observation. ?obs prop:cityid ?cityid filter regex(?cityid,'${req.params.cityid}').?obs prop:city ?city. ?obs prop:year ?year.?obs prop:humidity ?humidity.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});
//get humidity value by cityid 
app.get('/climate/humidity/cityid=:cityid', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?humidity from <${GRAPHDB_CONTEXT_TEST}> where {?obs a qb:Observation.  ?obs prop:cityid ?cityid filter(?cityid = ${req.params.cityid}).?obs prop:city ?city.?obs prop:year ?year.?obs prop:humidity ?humidity.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});

//get rainfall value by cityid
app.get('/climate/rainfall/cityid=:cityid', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?rainfall from <${GRAPHDB_CONTEXT_TEST}> where {?obs a qb:Observation. ?obs prop:cityid ?cityid filter(?cityid = ${req.params.cityid}).?obs prop:city ?city. ?obs prop:year ?year.?obs prop:rainfall ?rainfall.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});

//get temperature value by cityid
app.get('/climate/temperature/cityid=:cityid', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?temperature where {?obs a qb:Observation. ?obs prop:cityid ?cityid filter(?cityid = ${req.params.cityid}).?obs prop:city ?city. ?obs prop:year ?year.?obs prop:temperature ?temperature.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});

//get temperature value by cityid and yearid
app.get('/climate/temperature/cityid=:cityid/yearid=:yearid', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?temperature where {?obs a qb:Observation. ?obs prop:cityid ?cityid filter(?cityid = ${req.params.cityid}).?obs prop:yearid ?yearid filter(?yearid = ${req.params.yearid}).?obs prop:city ?city. ?obs prop:year ?year.?obs prop:temperature ?temperature.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});

//get temperature value by year
app.get('/climate/temperature/year=:year', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/climate/prop/>
        select ?city ?year ?temperature where {?obs a qb:Observation.?obs prop:city ?city. ?obs prop:year ?year filter(?year = ${req.params.year}).?obs prop:temperature ?temperature.}`, { transform: "toJSON" })
    .then((result) => {
        return res.json(result);
    })
    .catch((err) => {
        console.log(err);
    });
});

app.get('/climate')

app.listen(port, () => console.log(`Hello world app listening on port ${port}!`));     
