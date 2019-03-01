# MicroService for fetching data from a remote MongoDB database, and format it into CSV.

### Prerequisite:
You need to install Node 8.0 or higher. [see more](https://nodejs.org/en/download/)

## Setup:
In your terminal, type:  
```
git clone https://github.com/guillim/mongo2csv-API.git mongo2csv-API && cd mongo2csv-API && npm install
```  

Then configure your database:  
In the /config folder, rename _db_example.js_ into _db.js_ and fill with your credentials / IPs... Some examples are commented inside this file.
## Run:
```bash
npm run dev
```
Here you go ! Follow this link to see it working: [http://localhost:8000/status](http://localhost:8000/status)

Second method, for the ones who have docker installed:  

## Setup (with Docker):
In your terminal, type:  
```
git clone https://github.com/guillim/mongo2csv-API.git mongo2csv-API && cd mongo2csv-API
```  

Then configure your database:  
In the /config folder, rename _db_example.js_ into _db.js_ and fill with your credentials / IPs... Some examples are commented inside this file.
## Run (with Docker):
```bash
make up
```
Here you go ! Follow this link to see it working: [http://localhost:8000/status](http://localhost:8000/status)

If you want to stop:
```bash
make down
```

## MongoDB Configuration if you haven't any:
1. Go to https://mlab.com/ and create a free account (you have 500 Mo for free, enough for testing ! )
2. Create a database, and remember the name you chose
3. Add a user and a password, and copy the "mongod" URL
4. In the /config folder, rename _db_example.js_ into _db.js_ and replace password, username and databasename by the one you just created

![instructions](https://ibin.co/4GjY8K0VS5kf.png "Instructions to set up the free database")

## Customise your query to MongoDB
I wrote an example of  MongoDB query in the file note_routes.js
To fit you database Model, simply edit this file, and here you go !

In my example, here is how it works:
When I want to query the collection "results" with a parameter called postid equal to "fds4fjds657" and a second parameter equals to 45, I simply go to this route. Done  
[http://localhost:8000/getcsv/results/fds4fjds657/45](http://localhost:8000/getcsv/results/fds4fjds657/45)

It will simply do:  
```
db.collection('results').find({
  postid:'fds4fjds657',
  "crawlerFinishedAt": { $gt: new Date(new Date().setDate(new Date().getDate()-45))}
})
```
Like you can see in the note_routes file  



## BIG files: use the wetransfer option
If you have big files to send, maybe you would like to upload them through wetransfer.
1. Create you API key at [wetransfer](https://developers.wetransfer.com/)
2. Rename api_keys_example.js into api_keys.js and fill with you own API key
3. Simply use this route [http://localhost:8000/getcsvwetransfer/results/fds4fjds657/45](http://localhost:8000/getcsvwetransfer/results/fds4fjds657/45)

Also, if your file is too big, it may take a lot of time. The best solution is to get notified by email when the upload is finished. Then you can use this route instead:  
[http://localhost:8000/getcsvwetransferemail/results/fds4fjds657/45/my@email.com](http://localhost:8000/getcsvwetransferemail/results/fds4fjds657/45/my@email.com)

Note: Similar to what we did before, you will have to rename email_example.js into email.js and fill with you own credentials. It is configured to work with zoho and gmail, but could work for Outlook with very little change (see [here](https://ourcodeworld.com/articles/read/264/how-to-send-an-email-gmail-outlook-and-zoho-using-nodemailer-in-node-js) )




Your good to go !
----------------------------



### Thanks [Scott](https://github.com/scottdomes)
This was built following the [excellent article](https://medium.freecodecamp.org/building-a-simple-node-js-api-in-under-30-minutes-a07ea9e390d2)


### note for deployment on remote server using docker:
Once you have cloned your repo on your remote server, and you are running it with docker, you may need to connect it to anothe docker instance.
Shortly, you can see all docker networks available using
`docker network ls`
then you can inspect which containers are connected to each network using: (here the network is called `mongo2csv-api`)
`docker network inspect mongo2csv-api` 
If two containers want to communicate, the both must appear in the "containers" section 

Let's say you have a container called "gomanual" that isn't part of your "mongo2csv-api" network, then simply type:
`docker network connect mongo2csv-api gomanual`
