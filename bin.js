const transformCsv = require('.') /* the current working directory so that means main.js because of package.json */
let input = process.argv[2] /* what the user enters as first argument */
let output = process.argv[3] /* what the user enters as first argument */

    transformCsv(input,output)
