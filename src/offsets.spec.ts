import { getOffsetHandlers } from "./offsets";

describe('getOffsetHandlers', () => {
  let startWorkingOffset;
  let finishWorkingOffset;

  beforeEach(() => {
    const offsetHandlers = getOffsetHandlers();
    startWorkingOffset = offsetHandlers.startWorkingOffset;
    finishWorkingOffset = offsetHandlers.finishWorkingOffset;
  });

  it('should return undefined if not working the offset', () => {
    const partition = 0;

    expect(finishWorkingOffset(partition, 5)).toBeUndefined();
  });

  it('should return undefined if not the lowest value working', () => {
    const partition = 0;
    startWorkingOffset(partition, 0);
    startWorkingOffset(partition, 1);
    startWorkingOffset(partition, 2);
    startWorkingOffset(partition, 3);
    startWorkingOffset(partition, 4);

    expect(finishWorkingOffset(partition, 4)).toBeUndefined();
  });

  it('should return the input offset if no other offsets working', () => {
    const partition = 5;
    startWorkingOffset(partition, 1);

    const dataToCommit = finishWorkingOffset(partition, 1);
    expect(dataToCommit?.offset).toBe(1);
    expect(dataToCommit?.consumerGroupIds).toStrictEqual([]);
  });

  it('should return the highest finished offset, but lower than the lowest currently working offset', () => {
    const partition = 7;
    startWorkingOffset(partition, 0);
    startWorkingOffset(partition, 1);
    startWorkingOffset(partition, 2);
    startWorkingOffset(partition, 3);
    startWorkingOffset(partition, 4);

    expect(finishWorkingOffset(partition, 2)).toBe(undefined);
    expect(finishWorkingOffset(partition, 1)).toBe(undefined);
    expect(finishWorkingOffset(partition, 4)).toBe(undefined);

    startWorkingOffset(partition, 5);
    startWorkingOffset(partition, 6);
    startWorkingOffset(partition, 7);

    expect(finishWorkingOffset(partition, 3)).toBe(undefined);

    expect(finishWorkingOffset(partition, 0)?.offset).toBe(4);

    expect(finishWorkingOffset(partition, 6)).toBe(undefined);
    expect(finishWorkingOffset(partition, 7)).toBe(undefined);
    expect(finishWorkingOffset(partition, 5)?.offset).toBe(7);
  });

  it('should return one less than the lowest current working offset', () => {
    const partition = 18;
    startWorkingOffset(partition, 500);
    startWorkingOffset(partition, 510);
    startWorkingOffset(partition, 520);
    startWorkingOffset(partition, 530);

    expect(finishWorkingOffset(partition, 510)).toBe(undefined);
    expect(finishWorkingOffset(partition, 520)).toBe(undefined);
    expect(finishWorkingOffset(partition, 500)?.offset).toBe(529);
  });

  it('should return one less than the lowest current working offset with mixed partitions', () => {
    const partitionA = 18;
    const partitionB = 3;
    startWorkingOffset(partitionA, 500);
    startWorkingOffset(partitionA, 510);
    startWorkingOffset(partitionB, 500);
    startWorkingOffset(partitionB, 510);

    startWorkingOffset(partitionA, 520);
    startWorkingOffset(partitionA, 530);
    startWorkingOffset(partitionB, 520);
    startWorkingOffset(partitionB, 530);

    expect(finishWorkingOffset(partitionA, 510)).toBe(undefined);
    expect(finishWorkingOffset(partitionA, 520)).toBe(undefined);
    expect(finishWorkingOffset(partitionB, 510)).toBe(undefined);
    expect(finishWorkingOffset(partitionA, 500)?.offset).toBe(529);

    expect(finishWorkingOffset(partitionB, 520)).toBe(undefined);
    expect(finishWorkingOffset(partitionB, 500)?.offset).toBe(529);
  });
});