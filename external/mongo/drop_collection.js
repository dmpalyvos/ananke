conn = new Mongo();
collection = conn.getDB("ananke").getCollection("ananke");
collection.drop();
