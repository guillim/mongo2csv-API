const noteRoutes = require('./note_routes');
const customRoutes = require('./custom_routes');
/**
 * [gather all type of routes, that are defined in the routes_** folder]
 * @param  {[Object]} app [Express app]
 * @param  {[Object]} db  [MongoDB]
 */

module.exports = function(app, db) {
  noteRoutes(app, db);
  customRoutes(app, db);
  // Other route groups could go here, in the future
};
