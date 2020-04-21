const request = require('request')
const JSONStream = require('JSONStream')
const stringify = require('csv-stringify')
var CombinedStream = require('combined-stream')
const fs = require('fs')
const utils = require('../utils.js')
const rpcnodes = ['https://api.justyy.com']
// The readable.pipe() method attaches a Writable stream to the readable, 
// causing it to switch automatically into flowing mode and push all of its data to the attached Writable. 
// The flow of data will be automatically managed so that the destination Writable stream is not 
// overwhelmed by a faster Readable stream.

const columns = {
  "transfer": {
    count: 'count',
    amount: 'Amount',
    currency: 'Currency',
    to: 'To',
    from: 'From',
    timestamp: 'ts',
    memo: 'Memo',
    trx_id: 'trxid',
  },
  "curation_reward": {
    curator: 'Curator',
    reward: 'Reward',
    comment_author: 'Author',
    comment_permlink: 'Permlink',
    timestamp: 'ts',
  },
  "create_claimed_account": {
    creator: 'Creator',
    new_account_name: 'New account name',
    owner: 'owner',
    active: 'active',
    posting: 'posting',
    memo_key: 'memo_key',
    json_metadata: 'json_metadata',
    extensions: 'extensions',
  },
  "comment_benefactor_reward": {
    benefactor: 'benefactor',
    author: 'Author',
    permlink: 'Permlink',
    sbd_payout: 'SBD Payout',
    steem_payout: 'STEEM Payout',
    vesting_payout: 'Vest Payout',
  },
  "comment": {
    parent_author: 'Parent Author',
    parent_permlink: 'Parent Permlink',
    author: 'Author',
    permlink: 'Permlink',
    title: 'title',
    json_metadata: 'json_metadata',
  }
}

const getOptions = function (op, i) {
  return {
    header: i == 0 ? true : false,
    columns: columns[op]
  }
}

function wait (ms) {
  return new Promise((resolve, reject) => {
    setTimeout(() => { resolve() }, ms)
  })
}


async function downloadCsv (req, res, next) {
  var { operation, from, until, account } = req.query
  console.log(operation, from, until, account)
  // make sure its lowercase
  account = account.toLowerCase()
  from = new Date(from)
  until = new Date(until)
  const depth = 1000

  // should prob call this asychronously
  var OpCount = await utils.getOpCount(account)

  var fromDateReached = false
  var untilDateReached = false
  var i = 0
  var identifier = account + from.getTime().toString() + until.getTime().toString() + operation
  // In case of rpcnode timeouts, we will push the failed request to "failed" arr and try to recover it with a diff rpcnode
  var timeoutRequests = []
  const date1 = new Date()

  // date error handling
  // TODO no date prior to 2016 - creation of steem

  if (from > until) throw new Error('from > until')

  while (fromDateReached == false) {
    let rpcnode = rpcnodes[i % rpcnodes.length]
    let writeStream = fs.createWriteStream(`./${identifier}${i}.csv`)

    writeStream
    .on('error', function (err) {
      console.log(err)
      return res.send(JSON.stringify({ error: err }))
    })

    let start = OpCount - i * depth
    console.log(i + ' - '  + start)
    const data = { "jsonrpc":"2.0", "method":"condenser_api.get_account_history", "params":[account, start, depth], "id":1 }
    // request response is always a readable type of stream on a client

    requestBatch(rpcnode, data, i)
    .pipe(writeStream)

    await wait(300)
    i++
  }
  console.log('BINGO ' + i)

  // Retry for timedout requests
  // Another strategy for recovering data after a rpc times out, is having a default 5000-items depth and have next 
  // rpc node deal with x2 (10k items)
  for (let k = i; k < i + timeoutRequests.length; k++) {
    let writeStream = fs.createWriteStream(`./${identifier}${k}.csv`)
    let timeoutRequest = timeoutRequests[k - i]
    let failed_rpcnode = timeoutRequest.rpcnode
    let _rpcnodes = rpcnodes.filter((rpcnode) => rpcnode !== failed_rpcnode)
    let rpcnode = _rpcnodes[0]

    console.log('Trying to recover batch#' + timeoutRequest.batch)
    requestBatch(rpcnode, timeoutRequest.data, k)
    .pipe(writeStream)

    await wait(300)
    if (k >= i + timeoutRequests.lengh) console.log('Recovery loop ended')
  }

  // Timer for performance tests
  const date2 = new Date()
  const milisec = 1000
  const timediff = (date2 - date1) / milisec
  console.log(timediff)

  // Check disk for files before streaming out
  const dir = './'
  var fileCount = fs.readdirSync(dir).filter((file) => file.indexOf(identifier) > -1).length

  // Read stream combination
  var combinedStream = CombinedStream.create()
  for (let j = 0; j < fileCount; j++) {
    let path = `${identifier}${j}.csv`
    combinedStream.append(fs.createReadStream(path))
    fs.unlink(path, (err) => {
      if (err) return res.send(JSON.stringify({ error: err }))
      console.log(path + ' was deleted')
    })
  }
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=\"' + 'download-' + Date.now() + '.csv\"');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Pragma', 'no-cache');
  combinedStream.pipe(res)

  function requestBatch (rpcnode, data, i) {

    let start = 0
    try {
      start = data.params[1]
    } catch(err) {
      console.log(rpcnode, data, i)
      return res.send(JSON.stringify({ error: err }))
    }
    let depth = data.params[2]
    let stringifyWriteStream = stringify(getOptions(operation, i))
    request.post(rpcnode, { form: JSON.stringify(data) }) // .pipe(process.stdout)
    .on('data', (chunk) => {
      let textdata = chunk.toString()
      let json = {}
      try {
        json = JSON.parse(textdata)
      } catch(e) {
        // not an error msg
      }
      if (json.hasOwnProperty('error')) {
        console.log('Request failed at Batch #:' + i + ', OP#(start): ' + start + ' with rpcnode: ' + rpcnode + ' and with error: ' + json.error.message)
        let timeoutError = json.error.message.indexOf('Timeout') > -1
        if (timeoutError) {
          // WHY NOT RETURN HERE ANOTHER REQUEST (AKA READSTREAM) WITH A DIFF RPC NODE ?!
          timeoutRequests.push({ rpcnode: rpcnode, data: data, batch: i })
        } else {
          return res.send(JSON.stringify({ error: json.error.message }))
        }
      }
    })
    .pipe(JSONStream.parse('result.*', function (item) {
      let op = item[1].op[1]
      let timestamp = item[1].timestamp
      console.log(timestamp)
      let opNum = item[0]
      let trx_id = item[1].trx_id
      if (new Date(timestamp) < from) {
        fromDateReached = true
        return null
      } else if (new Date(timestamp) > until) {
        return null
      }
      op.timestamp = timestamp
      op.count = opNum
      op.trx_id = trx_id
      if (item[1].op[0] == 'transfer') {
        let currency = op.amount.indexOf('STEEM') > -1 ? 'STEEM' : 'SBD'
        op.amount = parseFloat(op.amount).toFixed(3)
        op.currency = currency
      }
      // return op
      return item[1].op[0] == operation ? op : null 
    }))
    .pipe(stringifyWriteStream)
    return stringifyWriteStream
  }
}

module.exports = {
  downloadCsv: downloadCsv
}