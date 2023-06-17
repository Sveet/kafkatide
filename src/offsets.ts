export function getOffsetHandlers() {
  const offsetsWorking: Map<number, number[]> = new Map();
  const offsetsFinished: Map<number, number[]> = new Map();

  const startWorkingOffset = (partition: number, offset: number) => {
    const offsets = offsetsWorking.get(partition) ?? [];
    offsets.push(offset);
    offsetsWorking.set(partition, offsets);
  };
  const finishWorkingOffset = (partition: number, offset: number): number => {
    // sanity check that we have the partition, offset in our working
    if (!offsetsWorking.get(partition)) {
      return undefined;
    }

    const newOffsetsFinished = offsetsFinished.get(partition) ?? [];
    newOffsetsFinished.push(offset);
    offsetsFinished.set(partition, newOffsetsFinished);

    let newOffsetsWorking = offsetsWorking.get(partition);
    const wasLowestOffsetWorking = offset <= Math.min(...newOffsetsWorking);

    newOffsetsWorking = newOffsetsWorking.filter((o) => o != offset);
    offsetsWorking.set(partition, newOffsetsWorking);

    if (wasLowestOffsetWorking) {
      const lowestOffsetWorking = Math.min(...newOffsetsWorking);
      if (isFinite(lowestOffsetWorking)) {
        offsetsFinished.set(
          partition,
          newOffsetsFinished.filter((o) => o >= lowestOffsetWorking),
        );
        return lowestOffsetWorking - 1;
      }
      const highestOffsetFinished = Math.max(...newOffsetsFinished);
      if (isFinite(highestOffsetFinished)) {
        offsetsFinished.set(partition, []);
        return highestOffsetFinished;
      }
      return offset;
    }
    return undefined;
  };
  return { startWorkingOffset, finishWorkingOffset };
}
