'use strict';

const { Transform } = require('stream');

module.exports = {
  jsonlBufferToObjectMode,
  objectModeToJsonlBuffer,
  streamCsv
};

function jsonlBufferToObjectMode() {
  return new Transform({
    writableObjectMode: false,
    readableObjectMode: true,
    transform(chunk, encoding, callback) {
      if (!this.buffer) {
        this.buffer = '';
      }
      this.buffer = `${this.buffer}${chunk}`;
      let lineArray = this.buffer.toString().split('\n');
      while (lineArray.length > 1) {
        try {
          const data = lineArray.shift();
          if (data === '') {
            continue;
          }
          const obj = JSON.parse(data);
          this.push(obj);
        } catch (e) {
          return callback(e);
        }
      }
      this.buffer = Buffer.from(lineArray[0], 'utf-8');
      callback();
    }
  });
}

function objectModeToJsonlBuffer() {
  return new Transform({
    writableObjectMode: true,
    readableObjectMode: false,
    transform(chunk, encoding, callback) {
      if (typeof chunk !== 'object') {
        return callback(new Error(`invalid data not in form of object: ${chunk}`));
      }
      let jsonl;
      try {
        jsonl = JSON.stringify(chunk);
      } catch (e) {
        return callback(e);
      }
      callback(null, `${jsonl}\n`);
    }
  });
}

/**
 *
 * @param {array} columns - optional columns to be included in the report
 * @param {boolean} showHeaders - optional display headers in output
 * @param {object} roundColumns - set to false for no rounding, Object keyed by column name, specify how many decimals
 * each column should have also with an optional default value
 * ex1. { default: 1, column1: 2, column2: 3 }
 * ex2. {column1: 2, column2: 3}
 * ex3. false
 * @returns {stream}
 *
 * strings are all wrapped in double-quotes
 * double-quotes in the strings are replaced with two double-quotes per the csv standard
 */
function streamCsv({ columns, showHeaders = true, roundColumns = false } = {}) {
  return new Transform({
    writableObjectMode: true,
    readableObjectMode: false,
    headersPrinted: false,
    transform(obj, encoding, callback) {
      columns = columns || Object.keys(obj);
      if (showHeaders && !this.headersPrinted) {
        const printColumns = columns.map(column => column.replace(/"/g,'""'));
        this.push(`"${ printColumns.join('","') }"\n`);
        this.headersPrinted = true;
      }
      let csvLine = [];
      for (const field of columns) {
        const data = obj[field];
        let dataToPush;
        if (data === undefined) {
          dataToPush = '';
        } else if (typeof data !== 'number') {
          dataToPush = typeof data === 'string' ? `"${ data.replace(/"/g, '""') }"` : `"${ data }"`;
        } else if (!roundColumns) {
          dataToPush = data;
        } else if (roundColumns[field] === undefined) {
          dataToPush = roundColumns.default === undefined ?
            data :
            roundToDigits(data, roundColumns.default).toFixed(roundColumns.default);
        } else {
          dataToPush = roundToDigits(data, roundColumns[field]).toFixed(roundColumns[field]);
        }
        csvLine.push(dataToPush);
      }
      callback(null, `${ csvLine.toString() }\n`);
    }
  });
}

function roundToDigits(number, digits) {
  return Number(Math.round(`${ number }e${ digits }`) + `e-${ digits }`);
};
