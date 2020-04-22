module.exports = function(app, db) {
  app.get('/status', (req, res) => {
    res.send('connected to the database')
  });
}
