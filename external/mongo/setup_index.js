conn = new Mongo();
collection = conn.getDB("ananke").getCollection("ananke");
collection.createIndex( { "ts" : -1 } )
collection.createIndex( { "type" : 1 } )
collection.createIndex( { "expired": 1 } )
collection.createIndex( { "sent": 1 } )
