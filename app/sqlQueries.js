const medalsQueryBuilder = `SELECT t1.year, t2.medals
                            FROM (SELECT year 
                                 FROM games 
                                 WHERE season=?) t1
                            LEFT JOIN (SELECT year, noc_name, COUNT(medal) AS medals
                                       FROM athletes,results,games,teams
                                       WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                         AND {medal_placeholder} AND season=? AND noc_name=?
                                       GROUP BY noc_name, year) t2
                            ON t1.year = t2.year
                            ORDER BY t1.year ASC;`;

const getMedalsMedalQuery = medalsQueryBuilder.replace(/{medal_placeholder}/g, 'medal=?');

const getMedalsEmptyQuery = medalsQueryBuilder.replace(/{medal_placeholder}/g, 'medal in (1,2,3)');

const topTeamQueryBuilder = `SELECT noc_name, COUNT(medal) AS medals
                             FROM athletes,results,games,teams
                             WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                AND {medal_placeholder} AND season=$season {year_placeholder}
                             GROUP BY noc_name {group_year}
                             HAVING medals >= (SELECT AVG(medals)
                                               FROM (SELECT noc_name, COUNT(medal) AS medals
                                                     FROM athletes,results,games,teams
                                                     WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                                        AND {medal_placeholder} AND season=$season {year_placeholder}
                                                     GROUP BY noc_name {group_year})
                                               )
                             ORDER BY medals DESC;`;

const getTopTeamsMedalYearQuery = topTeamQueryBuilder
  .replace(/{medal_placeholder}/g, 'medal=$medal')
  .replace(/{year_placeholder}/g, 'AND year=$year')
  .replace(/{group_year}/g, ', year');

const getTopTeamsMedalQuery = topTeamQueryBuilder
  .replace(/{medal_placeholder}/g, 'medal=$medal')
  .replace(/{year_placeholder}/g, '')
  .replace(/{group_year}/g, '');

const getTopTeamsYearQuery = topTeamQueryBuilder
  .replace(/{medal_placeholder}/g, 'medal in (1,2,3)')
  .replace(/{year_placeholder}/g, 'AND year=$year')
  .replace(/{group_year}/g, ', year');

const getTopTeamsEmptyQuery = topTeamQueryBuilder
  .replace(/{medal_placeholder}/g, 'medal in (1,2,3)')
  .replace(/{year_placeholder}/g, '')
  .replace(/{group_year}/g, '');

module.exports = {
  getMedalsMedalQuery,
  getMedalsEmptyQuery,
  getTopTeamsMedalYearQuery,
  getTopTeamsMedalQuery,
  getTopTeamsYearQuery,
  getTopTeamsEmptyQuery
};
