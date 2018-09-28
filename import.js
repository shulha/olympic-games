const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');
const fs = require('fs');

const rl = readline.createInterface({
  input: fs.createReadStream('databases/athlete_events.csv', {
    start: 111
  }),
  crlfDelay: Infinity
});

let sportsSet = new Set();
let eventsSet = new Set();
let athletesObj = {};
let teamsObj = {};
let gamesObj = {};
let resultsArray = [];

let prevGame = '';
let prevCity = '';

rl.on('line', (line) => {
  let arr = csvToArray(line).shift();

  let sex = (arr[2] !== 'NA') ? arr[2] : null;
  let age = +arr[3] ? +arr[3] : null;
  let params = {};
  if (+arr[4]) params['height'] = +arr[4];
  if (+arr[5]) params['weight'] = +arr[5];

  let noc = arr[7];

  let athleteName = arr[1].replace(/"+([^"]+)"+\s|"|(\(.*?\))*/g, '').trim();

  athletesObj[athleteName] = [
    sex,
    age,
    JSON.stringify(params),
    noc
  ];

  sportsSet.add(arr[12]);

  eventsSet.add(arr[13]);

  teamsObj[noc] = arr[6].replace(/-\d+$/g, '');

  let nextGame = arr[8];
  let nextCity = arr[11];
  if (nextGame === '1906 Summer') {} else if (prevGame === nextGame && prevCity !== nextCity) {
    gamesObj[nextGame] += ', ' + nextCity;
  } else {
    gamesObj[nextGame] = nextCity;
  }
  prevCity = nextCity;
  prevGame = nextGame;

  let medalFile = arr[14];
  if (nextGame !== '1906 Summer') {
    let medal;
    switch (medalFile) {
      case 'NA':
        medal = 0;
        break;
      case 'Gold':
        medal = 1;
        break;
      case 'Silver':
        medal = 2;
        break;
      case 'Bronze':
        medal = 3;
        break;
    }
    resultsArray.push([athleteName, arr[8], arr[12], arr[13], medal]);
  }
});

rl.on('close', function () {
  let db = new sqlite3.Database('databases/olympic_history.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) throw err;

    console.log('Connected to the olympic_history database.');
  });

  db.serialize(() => {
    insertSet(sportsSet, db, 'sports');

    insertSet(eventsSet, db, 'events');

    let resultsCnt = 0; let gamesCnt = 0; let teamsCnt = 0; let athletesCnt = 0;

    for (let oneRow of resultsArray) {
      db.run(
        'INSERT INTO results (athlete_id, game_id, sport_id, event_id, medal) VALUES (?,?,?,?,?)',
        oneRow,
        function (err) {
          if (err) throw err;

          resultsCnt += this.changes;
          console.log(`Rows inserted ${resultsCnt} to "results"`);
        }
      );
    }

    for (let game in gamesObj) {
      let cities = gamesObj[game];
      let gameArray = game.split(' ');
      let year = gameArray[0];
      let season = (gameArray[1] === 'Summer') ? 0 : 1;
      db.run(
        'INSERT INTO games (year, season, city) VALUES (?,?,?)',
        year, season, cities,
        function (err) {
          if (err) throw err;

          gamesCnt += this.changes;
          console.log(`Rows inserted ${gamesCnt} to "games"`);
        }
      );
    }

    for (let key in teamsObj) {
      db.run(
        'INSERT INTO teams (name, noc_name) VALUES (?,?)',
        teamsObj[key], key,
        function (err) {
          if (err) throw err;

          teamsCnt += this.changes;
          console.log(`Rows inserted ${teamsCnt} to "teams"`);
        }
      );
    }

    for (let athlete in athletesObj) {
      db.run(
        'INSERT INTO athletes (full_name, age, sex, params, team_id) VALUES (?,?,?,?,?)',
        athlete, athletesObj[athlete][0], athletesObj[athlete][1], athletesObj[athlete][2], athletesObj[athlete][3],
        function (err) {
          if (err) throw err;

          athletesCnt += this.changes;
          console.log(`Rows inserted ${athletesCnt} to "athletes"`);
        }
      );
    }
  });

  updateTable(db, 'noc_name', 'team');
  updateTable(db, 'full_name', 'athlete');
  updateTable(db, 'name', 'sport');
  updateTable(db, 'name', 'event');
  updateTable(db, 'year, season', 'game');

  db.close((err) => {
    if (err) throw err;

    console.log('Close the database connection.');
  });
});

function updateTable (db, colName, selectedTable) {
  let tmpArray = [];
  db.all('SELECT id, ' + colName + ' FROM ' + selectedTable + 's', [], (err, rows) => {
    if (err) throw err;

    if (selectedTable === 'game') {
      rows.forEach((row) => {
        let season = (row.season === 0) ? 'Summer' : 'Winter';
        tmpArray.push([row.id, row.year + ' ' + season]);
      });
    } else {
      rows.forEach((row) => {
        tmpArray.push([row.id, row[colName]]);
      });
    }

    let _table = 'results';
    if (selectedTable === 'team') {
      _table = 'athletes';
    }

    for (let data of tmpArray) {
      let sql = `UPDATE ${_table}
        SET ${selectedTable}_id = ?
        WHERE ${selectedTable}_id = ?`;

      db.run(sql, data, function (err) {
        if (err) throw err;

        console.log(`Row(s) updated: ${this.changes} in "${_table}"`);
      });
    }
  });
}

function insertSet (set, db, table) {
  let array = Array.from(set);
  let placeholders = array.map((arg) => '(?)').join(',');
  let sql = 'INSERT INTO ' + table + '(name) VALUES ' + placeholders;
  db.run(sql, array, function (err) {
    if (err) throw err;

    console.log(`Rows inserted ${this.changes} to "${table}"`);
  });
}

function csvToArray (text) {
  let p = ''; let row = ['']; let ret = [row]; let i = 0; let r = 0; let s = !0; let l;
  for (l of text) {
    if (l === '"') {
      if (s && l === p) row[i] += l;
      s = !s;
    } else if (l === ',' && s) l = row[++i] = '';
    else if (l === '\n' && s) {
      if (p === '\r') row[i] = row[i].slice(0, -1);
      row = ret[++r] = [l = '']; i = 0;
    } else row[i] += l;
    p = l;
  }
  return ret;
}
