"use strict";

class ContractError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "ContractError";
    this.code = code;
  }
}

function errorRecord(error) {
  if (!error) {
    return undefined;
  }
  const record = {
    name: String(error.name || "Error"),
    message: String(error.message || error).slice(0, 4000),
  };
  if (error.code) {
    record.code = String(error.code);
  }
  return record;
}

module.exports = {
  ContractError,
  errorRecord,
};
