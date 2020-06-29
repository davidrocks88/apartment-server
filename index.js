const sqlite3 = require('sqlite3').verbose();
const express = require('express');
const fs = require('fs');
const avalon = require('avalonbay-api');

const app = express();
const port = process.env.PORT | 3000;
const communitySchema = ["community_id", "name", "city", "state", "address", "type", "url", "count"];
const apartmentSchema = ["apartment_id", "community_id", "apartmentNumber", "apartmentAddress", "size", "beds", "baths", "floor"];
const priceSchema = ["apartment_id", "price", "date"];

app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening at http://localhost:${port}`));


// open the database
let db = new sqlite3.Database('./db/apartments.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the chinook database.');
});

process.on('exit', (code) => {
    db.close((err) => {
        if (err) {
            console.error(err.message);
        }
        console.log('Close the database connection.');
    });
})


app.get('/communities/states/:state/update', async (req, res) => {
    const communities = await avalon.searchState(req.params.state);
    const communityPlaceholders = communities.map(() => `(${communitySchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO communities (${communitySchema.join(", ")}) VALUES ${communityPlaceholders}`;
    const flattenedResponse = communities.map(c => Object.values(c)).flat();
    db.serialize(() => {
        db.run(statement, flattenedResponse, (err) => {
            if (err) {
                res.send(err);
            }
            res.send(200);
        })
    })
})

app.get('/communities', (req, res) => {
    const sql = `SELECT ${communitySchema.join(", ")} FROM communities`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(400);
        }
        else {
            res.send({communities: rows});
        }
    });
})

app.get('/communities/:communityId/update', async (req, res) => {
    const apartments = await avalon.searchCommunity(req.params.communityId);
    const apartmentPlaceholders = apartments.map(() => `(${apartmentSchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO apartments (${apartmentSchema.join(", ")}) VALUES ${apartmentPlaceholders}`;
    const flattenedResponse = apartments.map(a => [a.id, a.communityID, a.apartmentNumber, a.apartmentAddress, a.size, a.beds, a.baths, a.floor]).flat();
    db.serialize(() => {
        db.run(statement, flattenedResponse, (err) => {
            if (err) {
                res.send(err);
            }
            else {
                // Insert price
                const date = new Date();
                const dateStr = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
                const prices = apartments.map(a => [a.id, a.price, dateStr]);
                const pricePlaceholders = prices.map(() => `(${priceSchema.map(() => "?").join(", ")})`).join(", ");
                const priceStatement = `INSERT OR REPLACE INTO apartment_prices (${priceSchema.join(", ")}) VALUES ${pricePlaceholders}`;
                db.serialize(() => {
                    db.run(priceStatement, prices.flat(), (pricesErr) => {
                        if (err) {
                            res.send(err);
                        }
                        else {
                            res.send(200);
                        }
                    });
                })
            }
        })
    })
})

app.get('/communities/:communityId/apartments', (req, res) => {
    const sql = `SELECT ${apartmentSchema.join(", ")} FROM apartments WHERE community_id = "${req.params.communityId}"`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            res.send({apartments: rows});
        }
    });
})

app.get('/apartments/:apartmentId/prices', (req, res) => {
    const sql = `SELECT ${priceSchema.join(", ")} FROM apartment_prices WHERE apartment_id = "${req.params.apartmentId}"`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            res.send({apartments: rows});
        }
    });
})