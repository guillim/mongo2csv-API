const noteRoutes = require('./note_routes');
const customRoutes = require('./custom_routes');
const customRoutesStreaming = require('./custom_routes_streaming');

/**
 * [gather all type of routes, that are defined in the routes_** folder]
 * @param  {[Object]} app [Express app]
 * @param  {[Object]} db  [MongoDB]
 */

module.exports = function(app, db, done) {
  noteRoutes(app, db, done);
  customRoutes(app, db, done);
  customRoutesStreaming(app, db, done);
  // Other route groups could go here, in the future
};
