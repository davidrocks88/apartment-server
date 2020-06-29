async function asyncDbInsert(db, statement, body) {
    return new Promise((resolve, reject) => { 
        db.parallelize(() => {
            db.run(statement, body, (err) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(true);
                }
            })
        })
    })
}

Object.defineProperty(Array.prototype, 'chunk', {
    value: function(chunkSize) {
      var R = [];
      for (var i = 0; i < this.length; i += chunkSize)
        R.push(this.slice(i, i + chunkSize));
      return R;
    }
  });

const communitySchema = ["community_id", "name", "city", "state", "address", "type", "url", "count", "images"];
const apartmentSchema = ["apartment_id", "community_id", "apartmentNumber", "apartmentAddress", "size", "beds", "baths", "floor"];
const priceSchema = ["apartment_id", "price", "date"];
const stateSchema = ["id", "name"];

const insertCommunities = (db, communities) => {
    communities.forEach(community => community.images = community.images.join(","))
    const communityPlaceholders = communities.map(() => `(${module.exports.communitySchema.map(() => "?").join(", ")})`).join(", ");
    const statement = `INSERT OR REPLACE INTO communities (${module.exports.communitySchema.join(", ")}) VALUES ${communityPlaceholders}`;
    const flattenedResponse = communities.map(c => Object.values(c)).flat();

    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run(statement, flattenedResponse, (err) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(true);
                }
            })
        })
    })
}

const insertApartmentsAndPricesForToday = (db, apartments) => {
    return new Promise((resolve, reject) => {

        const apartmentChunks = apartments.chunk(50);

        const insertIntoApartmentsPromiseList = [];

        for (let apartmentChunk of apartmentChunks) {
            const apartmentPlaceholders = apartmentChunk.map(() => `(${apartmentSchema.map(() => "?").join(", ")})`).join(", ");
            const statement = `INSERT OR REPLACE INTO apartments (${apartmentSchema.join(", ")}) VALUES ${apartmentPlaceholders}`;
            const flattenedResponse = apartmentChunk.map(a => [a.id, a.communityID, a.apartmentNumber, a.apartmentAddress, a.size, a.beds, a.baths, a.floor]).flat();
            insertIntoApartmentsPromiseList.push(asyncDbInsert(db, statement, flattenedResponse));


            // Insert price
            const date = new Date();
            const dateStr = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
            const prices = apartmentChunk.map(a => [a.id, a.price, dateStr]);
            const pricePlaceholders = prices.map(() => `(${priceSchema.map(() => "?").join(", ")})`).join(", ");
            const priceStatement = `INSERT OR REPLACE INTO apartment_prices (${priceSchema.join(", ")}) VALUES ${pricePlaceholders}`; //possible bug, multiple copies
            insertIntoApartmentsPromiseList.push(asyncDbInsert(db, priceStatement, prices.flat()));
        }

        Promise.all(insertIntoApartmentsPromiseList)
            .then(resolve)
            .catch(reject);
    })
}



module.exports = {
    communitySchema,
    apartmentSchema,
    priceSchema,
    stateSchema,
    insertCommunities,
    insertApartmentsAndPricesForToday
}