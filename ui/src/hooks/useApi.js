import { useState, useEffect, useCallback, useRef } from 'react';

// Centralises all loading/error/data state so every component that calls an
// API function gets identical lifecycle behaviour without duplicating useState
// boilerplate. Components receive stable execute and data references.
export default function useApi(apiFn, options = {}) {
  const { immediate = false, interval = null } = options;

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  // Keep a stable reference to apiFn so the interval effect does not
  // re-register itself every render when an inline function is passed.
  const apiFnRef = useRef(apiFn);
  apiFnRef.current = apiFn;

  const execute = useCallback(async (...args) => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFnRef.current(...args);
      setData(response.data);
      setLastUpdated(new Date());
      return response.data;
    } catch (err) {
      // Normalise axios error shape into {code, message} so components
      // never need to inspect raw axios error internals.
      const apiError = err.response?.data?.error;
      setError({
        code: apiError?.code ?? 'UNKNOWN_ERROR',
        message: apiError?.message ?? err.message ?? 'An unexpected error occurred',
      });
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (immediate) {
      execute();
    }
  }, [immediate, execute]);

  useEffect(() => {
    if (!interval) return;
    const id = setInterval(() => execute(), interval);
    return () => clearInterval(id);
  }, [interval, execute]);

  return { data, loading, error, execute, lastUpdated };
}
