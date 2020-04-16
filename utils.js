const dsteem = require('dsteem')
const client = new dsteem.Client('https://api.hive.blog')
// https://api.hivekings.com
// https://anyx.io
function getClient () {
	return client
}
async function getOpCount (account) {
	let res = await client.database.call('get_account_history', [account, -1, 0])
	return parseFloat(res[0][0])
}
module.exports = {
	getClient: getClient,
	getOpCount: getOpCount
}