module.exports = {
  roots: [
    '<rootDir>/tests'
  ],
  coveragePathIgnorePatterns: [
    '/tests/',
    '/node_modules/'
  ],
  transform: {
    '^.+\\.tsx?$': 'ts-jest'
  },
  testRegex: '(\\.|/)(test|spec)\\.tsx?$',
  moduleFileExtensions: [
    'ts',
    'tsx',
    'js',
    'jsx',
    'json',
    'node'
  ],
  testEnvironment: 'node'
};
