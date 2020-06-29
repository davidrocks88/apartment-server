const sqlite3 = require('sqlite3').verbose();
const express = require('express');
const avalon = require('avalonbay-api');
const dbUtil = require("./db.js");
const cors = require('cors');

const app = express();
const port = process.env.PORT | 3001;

app.use(cors());
app.use((req, res, next) => {
    console.log(req.url);
    next();
})
app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening at http://localhost:${port}`));


// open the database
let db = new sqlite3.Database('./db/apartments.db', (err) => {
    if (err) {
        console.error(err.message);
    }
});

process.on('exit', (code) => {
    db.close((err) => {
        if (err) {
            console.error(err.message);
        }
    });
})

app.get('/communities/states/:state/update', async (req, res) => {
    const communities = await avalon.searchState(req.params.state);
    const communityPlaceholders = communities.map(() => `(${dbUtil.communitySchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO communities (${dbUtil.communitySchema.join(", ")}) VALUES ${communityPlaceholders}`;
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
    const sql = `SELECT ${dbUtil.communitySchema.join(", ")} FROM communities`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(400);
        }
        else {
            rows.forEach(row => row.images = row.images.split(","))
            res.send({ communities: rows });
        }
    });
})

app.get('/communities/:communityId/update', async (req, res) => {
    const apartments = await avalon.searchCommunity(req.params.communityId);
    const apartmentPlaceholders = apartments.map(() => `(${dbUtil.apartmentSchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO apartments (${dbUtil.apartmentSchema.join(", ")}) VALUES ${apartmentPlaceholders}`;
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
                const pricePlaceholders = prices.map(() => `(${dbUtil.priceSchema.map(() => "?").join(", ")})`).join(", ");
                const priceStatement = `INSERT OR REPLACE INTO apartment_prices (${dbUtil.priceSchema.join(", ")}) VALUES ${pricePlaceholders}`;
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
    const sql = `SELECT ${dbUtil.apartmentSchema.join(", ")} FROM apartments WHERE community_id = "${req.params.communityId}"`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            res.send({ apartments: rows });
        }
    });
})

app.get('/apartments/:apartmentId/prices', (req, res) => {
    const sql = `SELECT ${dbUtil.priceSchema.join(", ")} FROM apartment_prices WHERE apartment_id = "${req.params.apartmentId}"`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            const uniqueStrs = [... new Set(rows.map(a => `${a.apartment_id} | ${a.price} | ${a.date}`))];
            const uniqueObjects = uniqueStrs.map(s => {
                const arr = s.split('|').map(s => s.trim());
                return {
                    apartment_id: arr[0], price: Number(arr[1]), date: arr[2]
                }
            });
            res.send({ apartments: uniqueObjects });
        }
    });
})

app.get('/states', (req, res) => {
    const sql = `SELECT ${dbUtil.stateSchema.join(", ")} FROM states`;
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            res.send({ states: rows });
        }
    });
})

app.get('/states/update', async (req, res) => {
    const states = await avalon.getStates();
    const statePlaceholders = states.map(() => `(${dbUtil.stateSchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO states (${dbUtil.stateSchema.join(", ")}) VALUES ${statePlaceholders}`;
    const statesFlat = states.map(s => Object.values(s)).flat();
    db.serialize(() => {
        db.run(statement, statesFlat, (err) => {
            if (err) {
                res.send(err);
            }
            res.send(200);
        })
    })
})

app.get('/fullupdate', async (req, res) => {
    const sql = `SELECT ${dbUtil.stateSchema.join(", ")} FROM states`;
    db.all(sql, [], async (err, rows) => {
        if (err) {
            res.send(err);
        }
        else {
            try {
                const promiseList = rows.map(async state => await avalon.searchState(state.id));
                const communitiesByState = await Promise.all(rows.map(async state => await avalon.searchState(state.id)))
                const result = await Promise.all(communitiesByState.map(async c => dbUtil.insertCommunities(db, c)));

                if (result.reduce((prev, current) => prev && current) === true) {
                    const apartmentsByCommunity = await Promise.all(communitiesByState.flat().map(async c => await avalon.searchCommunity(c.id)));
                    const resultFromInsertingApartments = await dbUtil.insertApartmentsAndPricesForToday(db, apartmentsByCommunity.flat());
                    res.send(result);

                    // const result2 = await Promise.all(apartmentsByCommunity.map(async a => ))
                } else {
                    res.send(err);
                }
            } catch (err) {
                res.send(err);
            }

        }
    });
})