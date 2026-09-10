/** This process is the recorder — always capture when Mongo says Recording on. */
export function canProcessRecord(): boolean {
  return true;
}
