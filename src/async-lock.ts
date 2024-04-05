import Q from 'q';
import { AsyncLockOptions } from './async-lock-opts';
import { Callback, Fn } from './types';


const DEFAULT_TIMEOUT = 0;
const DEFAULT_MAX_PENDING = 1000;


export class AsyncLock {
    private _queue: Record<string, Fn[]> = {};
    private readonly _timeout: number;
    private readonly _maxPending: number;
    private _promise = Q;

    constructor(opts: AsyncLockOptions = {}) {
        this._timeout = opts.timeout || DEFAULT_TIMEOUT;
        this._maxPending = opts.maxPending || DEFAULT_MAX_PENDING;
    }

    /**
     * Acquire Locks
     *
     * @param {String|Array} key 	resource key or keys to lock
     * @param {function} fn 	async function
     * @param {function} cb 	(optional) callback function, otherwise will return a promise
     * @param {AsyncLockOptions} opts 	(optional) options
     */
    public acquire = (key: string | string[], fn: Fn, cb?: Callback, opts?: AsyncLockOptions): Q.Promise<unknown> | void => {
        if (Array.isArray(key)) {
            return this._acquireBatch(key, fn, cb, opts);
        }

        if (typeof (fn) !== 'function') {
            throw new Error('You must pass a function to execute');
        }

        let deferred: Q.Deferred<unknown> | null = null;
        if (typeof (cb) !== 'function') {
            opts = cb;
            cb = undefined;

            // will return a promise
            deferred = this._promise.defer();
        }

        opts = opts || {};

        let resolved = false;
        let timer: NodeJS.Timeout | null = null;

        const done = (locked: boolean, err?: unknown, ret?: unknown) => {
            if (locked && this._queue[key].length === 0) {
                delete this._queue[key];
            }

            if (!resolved) {
                if (!deferred) {
                    if (typeof (cb) === 'function') {
                        cb(err, ret);
                    }
                } else {
                    //promise mode
                    if (err) {
                        deferred.reject(err);
                    } else {
                        deferred.resolve(ret);
                    }
                }
                resolved = true;
            }

            if (locked) {
                //run next func
                if (!!this._queue[key]) {
                    this._queue[key]!.shift()!();
                }
            }
        };

        let exec = (locked: boolean) => {
            if (resolved) { // may due to timed out
                return done(locked);
            }

            if (timer) {
                clearTimeout(timer);
                timer = null;
            }

            // Callback mode
            if (fn.length === 1) {
                let called = false;
                fn((err: unknown, ret: unknown) => {
                    if (!called) {
                        called = true;
                        done(locked, err, ret);
                    }
                });
            } else {
                // Promise mode
                this._promise.try(() => {
                    return fn();
                })
                    .nodeify(function (err, ret) {
                        done(locked, err, ret);
                    });
            }
        };

        if (!this._queue[key]) {
            this._queue[key] = [];
            exec(true);
        } else if (this._queue[key].length >= this._maxPending) {
            done(false, new Error('Too many pending tasks'));
        } else {
            this._queue[key].push(function () {
                exec(true);
            });

            const timeout = opts.timeout || this._timeout;
            if (timeout) {
                timer = setTimeout(function () {
                    timer = null;
                    done(false, new Error('async-lock timed out'));
                }, timeout);
            }
        }

        if (deferred) {
            return deferred.promise;
        }
    };

    /**
     * Acquire Locks in batch mode
     *
     * @param {Array} keys 	resource keys to lock
     * @param {function} fn 	async function
     * @param {function} cb 	(optional) callback function, otherwise will return a promise
     * @param {AsyncLockOptions} opts 	(optional) options
     *
     * */
    private _acquireBatch(keys: string[], fn: Fn, cb?: Callback, opts?: AsyncLockOptions): Q.Promise<unknown> | void {
        if (typeof (cb) !== 'function') {
            opts = cb;
            cb = undefined;
        }

        const getFn = (key: string, fn: Fn) => {
            return (cb: Callback) => {
                this.acquire(key, fn, cb, opts);
            };
        };

        let fnx = fn;
        keys.reverse().forEach(function (key) {
            fnx = getFn(key, fnx);
        });

        if (typeof (cb) === 'function') {
            fnx(cb);
        } else {
            const deferred = this._promise.defer();
            fnx((err: unknown, ret: unknown) => {
                if (err) {
                    deferred.reject(err);
                } else {
                    deferred.resolve(ret);
                }
            });
            return deferred.promise;
        }
    };

    /*
     *	Whether there is any running or pending asyncFunc
     *
     *	@param {String} key (Optional)
     */
    public isBusy(key?: string) {
        if (!key) {
            return Object.keys(this._queue).length > 0;
        } else {
            return !!this._queue[key];
        }
    };
}

export default AsyncLock;
