const csv = require('csv-parser');
const fs = require('fs');
const chunkSize = 200000;
let rowWrite = 0;
module.exports = function (inputPath) {
    console.time("Extract and Transform");
    if (inputPath === undefined || inputPath === "") {
        console.error("invalid input or output path");
        console.log(`please specify inout,output path sample command : ./dbsa1t2 <input> <output>`);
        return process.exit(1);
    } else {
        console.log("Starting Script ");
        return Promise.resolve(Main(inputPath).then(obj => {
            console.log("Read Complete Start Writing JSON file");
        }).then(_ => {
            console.log(`successful writing json data : write ${rowWrite} rows `);
            console.timeEnd("Extract and Transform");
            return process.exit(0);
        }));
    }
}


async function Main(inputPath) {
    return await readCsv(inputPath);

}

async function readCsv(inputPath) {
    let data = [];
    let filesCount = 0;
    let len =0;
    await new Promise(function (resolve, reject) {
        console.log('processing csv data');

        fs.createReadStream(inputPath)
            .pipe(csv())
            .on('data', (row) => {
                // console.log(row);
                data.push(row);
                len++;
                if (data.length === chunkSize) {
                    filesCount++;
                    writeJSON(data,filesCount);
                    data = [];
                }
            })
            .on('end', () => {
                console.log("read complete")
                console.log(len);
                return resolve(writeJSON(data,filesCount));
            }).on('error', () => {
            return reject();
        });
    })
    fs.watchFile(inputPath, _ => {
        fs.stat(inputPath, function (err, stats) {
            console.log(stats.size);
        });
    })
    return data;
}

async function writeJSON(obj,count) {
    let yearObj = {};
    // {
    //     "ID": "2887628",
    //     "Date_Time": "11/01/2019 05:00:00 PM",
    //     "Year": "2019",
    //     "Month": "November",
    //     "Mdate": "1",
    //     "Day": "Friday",
    //     "Time": "17",
    //     "Sensor_ID": "34",
    //     "Sensor_Name": "Flinders St-Spark La",
    //     "Hourly_Counts": "300"
    // }
    let chunk = 1;
    let loading = Math.floor(obj.length / 10);
    for (let i = 0, c = 0; i < obj.length; i++, c++) {
        let day = `${obj[i].Mdate}/${obj[i].Month}/${obj[i].Year}`;
        if (yearObj[day] === undefined) {
            yearObj[day] = {};
        }
        if (yearObj[day]["hours"] === undefined) {
            yearObj[day]["hours"] = {};
        }
        if (yearObj[day]["hours"][obj[i].Time] === undefined) {
            yearObj[day]["hours"][obj[i].Time] = {};
        }
        if (yearObj[day]["hours"][obj[i].Time]["sensors"] === undefined) {
            yearObj[day]["hours"][obj[i].Time]["sensors"] = [];
        }
        yearObj[day]["Year"] = obj[i].Year;
        yearObj[day]["Month"] = obj[i].Month;
        yearObj[day]["Day"] = obj[i].Mdate;
        yearObj[day]["DayOfWeek"] = obj[i].Day;
        // {
        //     "hour": "01",
        //     "sensors": [
        //     {
        //         "id": "2886664",
        //         "sensorId": "4",
        //         "sensorName": "Town Hall (West)"
        //     },
        //     ...
        // ],
        //     "count": "117"
        // }
        yearObj[day]["hours"][obj[i].Time]["count"] = parseInt(obj[i].Hourly_Counts);
        yearObj[day]["hours"][obj[i].Time]["hour"] = parseInt(obj[i].Time);
        yearObj[day]["hours"][obj[i].Time]["sensors"].push({
            "id": obj[i].ID,
            "sensorId": obj[i].Sensor_ID,
            "sensorName": obj[i].Sensor_Name
        })
        if (c === loading) {
            // console.log(`${chunk * 10}%`)
            chunk++;
            c = 0;
        }
        rowWrite++
    }

    let output = [];
    let objectArray = Object.entries(yearObj);
    for (const [key, value] of objectArray) {
        let hours = [];
        for (const [key1, value1] of Object.entries(value.hours)) {
            hours.push(value1)
            value.hours = hours;
        }
        output.push(value)
    }
    await fs.writeFileSync(`jsonFiles/aggregated${count}.json`, JSON.stringify(output));

}