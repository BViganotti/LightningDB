export interface QueryResult {
  data: {
    columns: string[];
    rows: Record<string, unknown>[];
    numRows: number;
  };
}

export interface TestCase {
  name: string;
  run: () => Promise<TestResult>;
}

export interface TestResult {
  name: string;
  passed: boolean;
  expected?: string;
  actual?: string;
  error?: string;
  durationMs: number;
}

export interface SuiteResult {
  name: string;
  results: TestResult[];
  passed: number;
  failed: number;
  durationMs: number;
}
