'use strict';

/**
 Returns an object that treats MySQL's inabilities to do certain queries.

 @class QueryInterface
 @static
 @private
 */

const _ = require('lodash');
const sequelizeErrors = require('../../errors');

/**
  A wrapper that fixes MySQL's inability to cleanly remove columns from existing tables if they have a foreign key constraint.

  @param  {string} tableName     The name of the table.
  @param  {string} columnName    The name of the attribute that we want to remove.
  @param  {Object} options

  @private
 */
function removeColumn(tableName, columnName, options) {
  options = options || {};

  return this.sequelize.query(
    this.QueryGenerator.getForeignKeyQuery(tableName.tableName ? tableName : {
      tableName,
      schema: this.sequelize.config.database
    }, columnName),
    _.assign({ raw: true }, options)
  )
    .spread(results => {
      //Exclude primary key constraint
      if (!results.length || results[0].constraint_name === 'PRIMARY') {
        // No foreign key constraints found, so we can remove the column
        return;
      }
      return this.sequelize.Promise.map(results, constraint => this.sequelize.query(
        this.QueryGenerator.dropForeignKeyQuery(tableName, constraint.constraint_name),
        _.assign({ raw: true }, options)
      ));
    })
    .then(() => this.sequelize.query(
      this.QueryGenerator.removeColumnQuery(tableName, columnName),
      _.assign({ raw: true }, options)
    ));
}


function removeConstraint(tableName, constraintName, options) {
  const sql = this.QueryGenerator.showConstraintsQuery(tableName.tableName ? tableName : {
    tableName,
    schema: this.sequelize.config.database
  }, constraintName);

  return this.sequelize.query(sql, Object.assign({}, options, { type: this.sequelize.QueryTypes.SHOWCONSTRAINTS }))
    .then(constraints => {
      const constraint = constraints[0];
      let query;
      if (constraint && constraint.constraintType) {
        if (constraint.constraintType === 'FOREIGN KEY') {
          query = this.QueryGenerator.dropForeignKeyQuery(tableName, constraintName);
        } else {
          query = this.QueryGenerator.removeIndexQuery(constraint.tableName, constraint.constraintName);
        }
      } else {
        throw new sequelizeErrors.UnknownConstraintError({
          message: `Constraint ${constraintName} on table ${tableName} does not exist`,
          constraint: constraintName,
          table: tableName
        });
      }

      return this.sequelize.query(query, options);
    });
}

/* =========================================================
 * BEGIN MashTheKeys Update to add function support in mysql
 * ========================================================= */
/**
 * A wrapper that allows MySQL's query delimiter to be altered during function creation.
 *
 * @param {String}    functionName
 * @param {Object[]}  params
 * @param {String}    returnType
 * @param {String}    language
 * @param {String}    body
 * @param {String[]}  optionsArray
 * @param {Object}    options
 *
 * @private
 */
function createFunction(functionName, params, returnType, language, body, optionsArray, options) {
  const delimiter = "__END_FUNCTION__";

  const functionSql = this.QueryGenerator.createFunction(functionName, params, returnType, language, body, optionsArray);

  if (functionSql) {
    // Mysql uses multiple statements to alter the query delimiter while defining a function
    return this.sequelize.query(`DELIMITER ${delimiter};`, options).then(() => {
      return this.sequelize.query(`${functionSql}\n${delimiter}`, options).then(() => {
        return this.sequelize.query('DELIMITER ;', options)
      })
    });

  } else {
    return Promise.resolve();
  }
}
/* =========================================================
 * END MashTheKeys Update to add function support in mysql
 * ========================================================= */

exports.createFunction = createFunction;
exports.removeConstraint = removeConstraint;
exports.removeColumn = removeColumn;
