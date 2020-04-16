const express = require('express')
const router = express.Router()
const controllers = require('../controllers')

router.get('/csv', controllers.downloadCsv)

module.exports = router