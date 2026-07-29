(function attachAutopresenterControlAuth(root) {
  'use strict';

  const STORAGE_KEY = 'autopresenter-control-token';
  const ignoreError = () => {};

  function remember(token, sessionStorage, persistentStorage, onError = ignoreError) {
    sessionStorage.setItem(STORAGE_KEY, token);
    try {
      persistentStorage.setItem(STORAGE_KEY, token);
    } catch (error) {
      onError('Не удалось сохранить доступ для следующего запуска:', error);
    }
  }

  function restore(sessionStorage, persistentStorage, onError = ignoreError) {
    const sessionToken = sessionStorage.getItem(STORAGE_KEY) || '';
    if (sessionToken) return sessionToken;

    let persistedToken = '';
    try {
      persistedToken = persistentStorage.getItem(STORAGE_KEY) || '';
    } catch (error) {
      onError('Не удалось прочитать сохранённый доступ:', error);
    }
    if (persistedToken) sessionStorage.setItem(STORAGE_KEY, persistedToken);
    return persistedToken;
  }

  function forget(sessionStorage, persistentStorage, onError = ignoreError) {
    sessionStorage.removeItem(STORAGE_KEY);
    try {
      persistentStorage.removeItem(STORAGE_KEY);
    } catch (error) {
      onError('Не удалось удалить сохранённый доступ:', error);
    }
  }

  root.AutopresenterControlAuth = Object.freeze({
    storageKey: STORAGE_KEY,
    remember,
    restore,
    forget,
  });
})(globalThis);
