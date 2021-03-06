const express = require('express');
const {graphDBEndpoint, dc} = require('../../config/dbconfig');
const router = express.Router();

//getIndustryByCityId
router.get('/industry/city/:cityid', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/prop/>
        select ?city ?year ?industry
        where {
            values ?cityid {'${req.params.cityid}'}
            ?obs a qb:Observation. 
            ?obs prop:cityid ?cityid.
            ?obs prop:city ?city. 
            ?obs prop:year ?year.
            ?obs prop:industry ?industry.
        }`, { transform: "toJSON" })        
    .then((result) => {
        finalResult = JSON.parse(JSON.stringify(result).split('"industry":').join('"value":'));
        return res.json(finalResult);
    })
    .catch((err) => {
        console.log(err);
    });
})

//getIndustryByYear
router.get('/industry/year/:year', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/prop/>
        select ?city ?year ?industry 
        where {
            ?obs a qb:Observation.
            ?obs prop:city ?city.
            ?obs prop:year ?year filter(?year = ${req.params.year}).
            ?obs prop:industry ?industry.
        }`, { transform: "toJSON" })
    .then((result) => {
        finalResult = JSON.parse(JSON.stringify(result).split('"industry":').join('"value":'));
        return res.json(finalResult);
    })
    .catch((err) => {
        console.log(err);
    });
})

//getIndustryyCityYear
router.get('/industry/city/:cityid/year/:year', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/prop/>
        select ?city ?year ?industry where {
            values ?cityid {'${req.params.cityid}'}
            ?obs a qb:Observation. ?obs prop:city ?city. ?obs prop:cityid ?cityid.
            ?obs prop:year ?year filter(?year = ${req.params.year}).
            ?obs prop:industry ?industry.
        }`, { transform: "toJSON" })
    .then((result) => {
        finalResult = JSON.parse(JSON.stringify(result).split('"industry":').join('"value":'));
        return res.json(finalResult);
    })
    .catch((err) => {
        console.log(err);
    });
})

//getIndustryByCityPeriod

router.get('/industry/city/:cityid/fyear/:fyear/tyear/:tyear', (req, res) => {
    graphDBEndpoint
    .query(
        `PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prop: <http://www.sda-research.ml/dc/prop/>
        select ?city ?year ?industry where {
            values ?cityid {'${req.params.cityid}'}
            ?obs a qb:Observation. ?obs prop:city ?city. ?obs prop:cityid ?cityid.
            ?obs prop:year ?year filter(?year >= ${req.params.fyear} && ?year <= ${req.params.tyear}).
            ?obs prop:industry ?industry.
        }`, { transform: "toJSON" })        
    .then((result) => {
        finalResult = JSON.parse(JSON.stringify(result).split('"industry":').join('"value":'));
        return res.json(finalResult);
    })
    .catch((err) => {
        console.log(err);
    });
})

module.exports = router;