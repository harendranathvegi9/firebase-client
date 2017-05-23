//import * as admin from "firebase-admin";

const fs = require('fs');
let notificationSent = {};
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://truthfinder-1ff0a.firebaseio.com"
});

const Redshift = require('node-redshift');
const mysql = require('mysql');
const querystring = require('querystring');
const http = require('http');
const uuid = require('uuid');

const eventsServiceClient = {
    host: 'events.cd4uamqthjmb.us-east-1.redshift.amazonaws.com',
    user: 'readonly',
    port: '5432',
    password: '9834hf8ybnu223xapmSADkf2',
    database: 'events'
};

const accountServiceClient = {
    host: '172.20.21.7',
    user: 'readonly',
    password: 'HzYpUddU3WxGfsUNzVMyZ3',
    database: 'accounts'
};

let options = { rawConnection: true };
const redshiftClient = new Redshift(eventsServiceClient, options);
module.exports = redshiftClient;

//Get the list of customers we sent notifications today (remporarily doing this because events are not real time))
fs.readFile(new Date().toISOString().slice(0, 10) + "-notifications_sent.txt", 'utf8', function(err, data) {
    if (err) throw err;
    let sent_notifications = data.split("\n");

    for (let n in sent_notifications) {
        if (sent_notifications[n] != '') {
            let c = JSON.parse(sent_notifications[n]).customerId;
            notificationSent[c] = c;
        }
    }
    console.log(notificationSent);
    console.log(Object.keys(notificationSent).map(function(k) {
        return notificationSent[k]
    }).join(","));

    const customerQuery = "WITH google_play_customers AS( SELECT \"customer:id\" FROM \"customer:register\" c JOIN \"session:web\"    s USING(\"session:id\") WHERE \"domain\" = 'api.truthfinder.com'    AND c.    \"event:created\"    BETWEEN CURRENT_DATE - INTERVAL '1 days'    AND CURRENT_DATE AND s.\"session:created\" >= CURRENT_DATE - INTERVAL '2 days'    AND \"customer:id\"    NOT IN (SELECT \"customer:id\"        FROM \"communication:push:outbound\"        JOIN \"session:worker\"        USING(\"session:id\") WHERE worker_slug = 'firebase_notification')) SELECT DISTINCT \"customer:id\" customer, \"report:search_pointer\" pointer FROM \"usage:report:view\" WHERE \"customer:id\" IN (SELECT \"customer:id\"    FROM google_play_customers WHERE \"customer:id\" NOT IN (" + Object.keys(notificationSent).map(function(k) {
        return notificationSent[k]
    }).join(",") + ") ) AND \"report:search_pointer\" IS NOT NULL AND \"report:type\" = 'person' AND \"event:created\" BETWEEN CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE LIMIT 10";
    console.log(customerQuery);

    // THIS IS THE REAL QUERY
    // let queryString = "WITH google_play_customers AS( SELECT \"customer:id\" FROM \"customer:register\" c JOIN \"session:web\"    s USING(\"session:id\") WHERE \"domain\" = 'api.truthfinder.com'    AND c.    \"event:created\"    BETWEEN CURRENT_DATE - INTERVAL '1 days'    AND CURRENT_DATE AND s.\"session:created\" >= CURRENT_DATE - INTERVAL '2 days'    AND \"customer:id\"    NOT IN (SELECT \"customer:id\"        FROM \"communication:push:outbound\"        JOIN \"session:worker\"        USING(\"session:id\") WHERE worker_slug = 'firebase_notification')) SELECT DISTINCT \"customer:id\" customer, \"report:search_pointer\" pointer FROM \"usage:report:view\" WHERE \"customer:id\" IN (SELECT \"customer:id\"    FROM google_play_customers) AND \"report:search_pointer\" IS NOT NULL AND \"report:type\" = 'person' AND \"event:created\" BETWEEN CURRENT_DATE - INTERVAL '1 day' AND CURRENT_DATE LIMIT 10";

    //TESTING BRIANS PHONE
    //let queryString = "SELECT DISTINCT \"customer:id\" customer, \"report:search_pointer\" pointer FROM \"usage:report:view\" WHERE  \"report:search_pointer\" IS NOT NULL  AND \"report:type\" = 'person' AND \"customer:id\"  = 45523182";
    //let queryString = "SELECT DISTINCT \"customer:id\" customer, \"report:search_pointer\" pointer FROM \"usage:report:view\" WHERE  \"report:search_pointer\" IS NOT NULL  AND \"report:type\" = 'person' AND \"customer:id\"  = 45046671";


    redshiftClient.connect(function(err) {
        if (err) throw err;

        else {
            redshiftClient.query(customerQuery, options, function(err, data) {
                if (err) throw err;
                else {
                    for (let d in data.rows) {
                        console.log('customerID: ', data.rows[d].customer);
                        console.log('Pointer: ', data.rows[d].pointer);

                        /* CALL DS for each poitner and get the comp data */

                        let post_data = querystring.stringify({
                            'pointer': data.rows[d].pointer
                        });

                        // An object of options to indicate where to post to
                        let post_options = {
                            host: '172.20.20.53',
                            port: '8000',
                            path: '/report/person/comp?raw-results=true',
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'X-Brand-Identity': 'TruthFinder'
                            }
                        };

                        // Set up the request
                        let post_req = http.request(post_options, function(res) {
                            res.setEncoding('utf8');
                            let str = '';

                            res.on('data', function(chunk) {
                                str += chunk;
                            });

                            res.on('end', function() {

                                if (!JSON.parse(str).results) return;

                                //console.log(JSON.parse(str).results[0].bankruptcies);
                                if (JSON.parse(str).results[0].bankruptcies.length) {
                                    console.log('This customer has a report with bankrupcy ' + data.rows[d].customer);
                                    console.log('Calling account service for the customer data');

                                    //ACCOUNT SERVICE CALL
                                    let options = {
                                        host: '172.20.21.13',
                                        port: '8000',
                                        path: '/v1/customers/' + data.rows[d].customer
                                    };

                                    let customer = '';
                                    let firebase_token = '';

                                    callback = function(response) {

                                        response.on('data', function(chunk) {
                                            customer += chunk;
                                        });

                                        response.on('end', function() {
                                            //console.log(customer);
                                            console.log(JSON.parse(customer).id);
                                            firebase_token = JSON.parse(customer).data.devices[0].firebase_token;
                                            if (!firebase_token) {
                                                console.log('no firebase token set for customer ' + JSON.parse(customer).id);
                                                return;
                                            }

                                            console.log("Firebase Token:" + firebase_token);

                                            //FIREBASE 
                                            let reportName = JSON.parse(str).results[0].names[0].display;

                                            // These registration tokens come from the client FCM SDKs.
                                            let registrationTokens = [
                                                firebase_token
                                            ];

                                            let payload = {
                                                data: {
                                                    title: "View " + reportName + "'s report now!",
                                                    content: "It looks like " + reportName + " may have had issues with money in the past.",
                                                    level: "info"
                                                },
                                                notification: {
                                                    title: "NOTICE: New Information available",
                                                    body: "We’ve uncovered more data in " + reportName + "'s report"
                                                }
                                            };

                                            // Send a message to the devices corresponding to the provided
                                            // registration tokens.
                                            admin.messaging().sendToDevice(registrationTokens, payload)
                                                .then(function(response) {

                                                    console.log("Successfully sent message:", JSON.stringify(response));
                                                    let notification_log = { firebase_token: firebase_token, customerId: JSON.parse(customer).id.toString(), payload: payload, response: JSON.stringify(response) };

                                                    fs.appendFile(new Date().toISOString().slice(0, 10) + "-notifications_sent.txt", "\n" + JSON.stringify(notification_log), function(err) {
                                                        if (err) {
                                                            return console.log(err);
                                                        }
                                                        console.log("Firebase token saved to file!");
                                                    });

                                                    let session_id = uuid.v4();
                                                    let created = new Date().toISOString().slice(0, 19) + '+00:00';

                                                    let session_event = "{\"worker_slug\": \"firebase_notification\",     \"service_slug\": \"firebase_notification\",    \"session\": {        \"created\": \"" + created + "\",       \"brand_slug\": \"TruthFinder\",        \"id\": \"" + session_id + "\",     \"type\": \"worker\"      },      \"version\": \"1.1.1\",     \"connection\": {},     \"event\": {      \"created\": \"" + created + "\",       \"id\": \"" + session_id + "\",     \"type\": \"session:worker\"      }}";
                                                    let notification_event = "{\"session\": {\"created\": \"" + created + "\",        \"brand_slug\": \"TruthFinder\",        \"id\": \"" + session_id + "\",        \"type\": \"worker\"    },    \"event\": {        \"created\": \"" + created + "\",        \"id\": \"" + session_id + "\",        \"type\": \"communication:push:outbound\"    },    \"customer\":" + customer + ",    \"content\": \"firebase notification\",    \"subscriber\": \"firebase notification\",    \"communication\": {        \"agent\": \"firebase notification\",        \"notes\": \"firebase notification\",        \"reason\": \"firebase notification\",        \"resolution\": \"firebase notification\"    }}";

                                                    //EMIT EVENTS HERE BY POSTING BOTH AND WRITE TO FILE AS WELL
                                                    let options = {
                                                        host: '172.20.16.233',
                                                        path: '/',
                                                        port: '3002',
                                                        method: 'POST'
                                                    };

                                                    callback = function(response) {
                                                        let str = ''
                                                        response.on('data', function(chunk) {
                                                            str += chunk;
                                                        });

                                                        response.on('end', function() {
                                                            console.log(str);
                                                        });
                                                    }

                                                    let req = http.request(options, callback);
                                                    req.write(session_event);
                                                    req.end();

                                                    fs.appendFile(new Date().toISOString().slice(0, 10) + "-session_events.txt", "\n" + JSON.stringify(JSON.parse(session_event)), function(err) {
                                                        if (err) {
                                                            return console.log(err);
                                                        }
                                                        console.log("The session event saved to file!");
                                                    });

                                                    let req2 = http.request(options, callback);
                                                    req2.write(notification_event);
                                                    req2.end();

                                                    fs.appendFile(new Date().toISOString().slice(0, 10) + "-notification_events.txt", "\n" + JSON.stringify(JSON.parse(notification_event)), function(err) {
                                                        if (err) {
                                                            return console.log(err);
                                                        }
                                                        console.log("The notification event saved to file!");
                                                    });
                                                })
                                                .catch(function(error) {
                                                    console.log("Error sending message:", error);
                                                });
                                        });
                                    }
                                    http.request(options, callback).end();
                                } else {
                                    console.log('No good premium data found');
                                }
                            });
                        });

                        // post the data to data service
                        post_req.write(post_data);
                        post_req.end();
                    }
                    redshiftClient.close();
                }
            });
        }
    });
});
