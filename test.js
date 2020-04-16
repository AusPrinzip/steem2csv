const fs = require('fs');
const file = fs.createWriteStream('./big.file');
const utils = require('./utils')
const client = utils.getClient()

// for(let i=0; i<= 1e6; i++) {
//   file.write('Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n');
// }

// file.end();
function wait () {
	return new Promise((resolve, reject) => {
		setTimeout(function () { resolve() }, 2000)
	})
}

async function test (account) {
	return new Promise((resolve, reject) => {
		client.database.call('get_account_history', ['likwid', 165088, 1])
		.then((res) => {
			console.log(res)
			// let filtered = res.filter((el) => el[1].op[0] == ('transfer'))
			// filtered = filtered.map((el) => el[1].op[1])
			// filtered.forEach((op) => {
			// 	console.log(parseFloat(op.amount).toFixed(3))
			// })
		})
	})
}

test()