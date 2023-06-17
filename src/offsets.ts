export function getOffsetHandlers() {
  const offsetsWorking: Map<number, { offset: number; consumerGroupId?: string }[]> = new Map();
  const offsetsFinished: Map<number, { offset: number; consumerGroupId?: string }[]> = new Map();

  const startWorkingOffset = (partition: number, offset: number, consumerGroupId?: string) => {
    const offsets = offsetsWorking.get(partition) ?? [];
    offsets.push({ offset, consumerGroupId });
    offsetsWorking.set(partition, offsets);
  };
  const finishWorkingOffset = (
    partition: number,
    offset: number,
    consumerGroupId?: string,
  ): { offset: number; consumerGroupIds: string[] } => {
    // sanity check that we have the partition, offset in our working
    if (!offsetsWorking.get(partition)) {
      return undefined;
    }

    const newOffsetsFinished = offsetsFinished.get(partition) ?? [];
    newOffsetsFinished.push({ offset, consumerGroupId });
    offsetsFinished.set(partition, newOffsetsFinished);

    let newOffsetsWorking = offsetsWorking.get(partition);
    const wasLowestOffsetWorking = offset <= Math.min(...newOffsetsWorking.map((o) => o.offset));

    newOffsetsWorking = newOffsetsWorking.filter((o) => o.offset != offset);
    offsetsWorking.set(partition, newOffsetsWorking);

    if (wasLowestOffsetWorking) {
      const lowestOffsetWorking = Math.min(...newOffsetsWorking.map((o) => o.offset));
      if (isFinite(lowestOffsetWorking)) {
        offsetsFinished.set(
          partition,
          newOffsetsFinished.filter((o) => o.offset >= lowestOffsetWorking),
        );
        return {
          offset: lowestOffsetWorking - 1,
          consumerGroupIds: newOffsetsFinished
            .filter((o) => o.offset < lowestOffsetWorking)
            .map((o) => o.consumerGroupId)
            .filter((o) => !!o),
        };
      }
      const highestOffsetFinished = Math.max(...newOffsetsFinished.map((o) => o.offset));
      if (isFinite(highestOffsetFinished)) {
        offsetsFinished.set(partition, []);
        return {
          offset: highestOffsetFinished,
          consumerGroupIds: newOffsetsFinished.map((o) => o.consumerGroupId).filter((o) => !!o),
        };
      }
      return { offset, consumerGroupIds: [consumerGroupId].filter((o) => !!o) };
    }
    return undefined;
  };
  return { startWorkingOffset, finishWorkingOffset };
}