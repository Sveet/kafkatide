import { Observable, first, switchMap } from 'rxjs';

// https://stackoverflow.com/a/52381176
export function waitFor<T>(signal: Observable<any>) {
  return (source: Observable<T>) => signal.pipe(
    first(),
    switchMap(_ => source),
  );
}