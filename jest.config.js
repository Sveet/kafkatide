const config = {
  "moduleFileExtensions": [
    "js",
    "json",
    "ts"
  ],
  "rootDir": ".",
  "testRegex": ".*\\.spec\\.ts$",
  "transform": {
    "^.+\\.(t|j)s$": "ts-jest"
  },
  "collectCoverage": true,
  "collectCoverageFrom": [
    "./src/**/*.(t|j)s"
  ],
  "coverageThreshold": {
    "global": {
      "lines": 90
    }
  },
  "coverageDirectory": "./coverage",
  "testEnvironment": "node",
}

module.exports = config;