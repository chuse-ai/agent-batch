import { Data } from "effect";

/** Input a module refuses as the contract refuses invalid input, with each problem's path. */
export class InvalidInput extends Data.TaggedError("InvalidInput")<{
  problems: readonly { message: string; path: (string | number)[] }[];
}> {}
