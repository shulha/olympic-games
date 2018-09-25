const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');
const fs = require('fs');

const rl = readline.createInterface({
  input: fs.createReadStream('databases/sandbox.csv', {
    start: 80
  }),
  crlfDelay: Infinity
});

let athletesSet = new Set();
let sportsSet = new Set();
let eventsSet = new Set();
let teamsObj = {};
let gamesObj = {};

let prevGame = '';
let prevCity = '';

rl.on('line', (line) => {
  let arr = line.split(',');

  let sex = (arr[2] !== 'NA') ? arr[2] : null;
  let age = +arr[3] ? +arr[3] : null;

  let params = {};
  if (+arr[4]) params['height'] = +arr[4];
  if (+arr[5]) params['weight'] = +arr[5];

  let athleteName = arr[1].replace(/"+([^"]+)"+\s|"|(\(.*?\))*/g, '').trim();

  let member = {
    full_name: arr[1],
    sex,
    age,
    params: JSON.stringify(params)
  };

  athletesSet.add(member);

  sportsSet.add(arr[12]);

  eventsSet.add(arr[13]);

  teamsObj[arr[7]] = arr[6].replace(/-\d+$/g, '');

  let nextGame = arr[8];
  let nextCity = arr[11];
  if (nextGame === '1906 Summer') {} else if (prevGame === nextGame && prevCity !== nextCity) {
    gamesObj[nextGame] += ', ' + nextCity;
  } else {
    gamesObj[nextGame] = nextCity;
  }
  prevCity = nextCity;
  prevGame = nextGame;
});

rl.on('close', function () {
  let db = new sqlite3.Database('databases/olympic_history.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
      return console.error(err.message);
    }
    console.log('Connected to the olympic_history database.');
  });

  // console.log(athletesSet);
  // console.log(sportsSet);
  // console.log(eventsSet);
  // console.log(teamsObj);
  // console.log(gamesObj);

  // insertSet(sportsSet, db);
  // insertSet(eventsSet, db);

  db.close((err) => {
    if (err) {
      return console.error(err.message);
    }
    console.log('Close the database connection.');
  });
});

function insertSet (set, db) {
  let array = Array.from(set);
  let placeholders = array.map((arg) => '(?)').join(',');
  let sql = 'INSERT INTO sports(name) VALUES ' + placeholders;
  db.run(sql, array, function (err) {
    if (err) {
      return console.error(err.message);
    }
    console.log(`Rows inserted ${this.changes}`);
  });
}
