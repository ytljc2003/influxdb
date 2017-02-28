import {createStore, applyMiddleware, compose} from 'redux';
import {combineReducers} from 'redux';
import thunkMiddleware from 'redux-thunk';
import makeQueryExecuter from 'src/shared/middleware/queryExecuter';
import resizeLayout from 'src/shared/middleware/resizeLayout';
import * as dataExplorerReducers from 'src/data_explorer/reducers';
import * as sharedReducers from 'src/shared/reducers';
import rulesReducer from 'src/kapacitor/reducers/rules';
import dashboardUI from 'src/dashboards/reducers/ui';
import usersReducer from 'src/users/reducers/users';
import persistStateEnhancer from './persistStateEnhancer';

const rootReducer = combineReducers({
  ...sharedReducers,
  ...dataExplorerReducers,
  rules: rulesReducer,
  users: usersReducer,
  dashboardUI,
});

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;

export default function configureStore(initialState) {
  const createPersistentStore = composeEnhancers(
    persistStateEnhancer(),
    applyMiddleware(thunkMiddleware, makeQueryExecuter(), resizeLayout),
  )(createStore);


  // https://github.com/elgerlambert/redux-localstorage/issues/42
  // createPersistantStore should ONLY take reducer and initialState
  // any store enhancers must be added to the compose() function.
  return createPersistentStore(
    rootReducer,
    initialState,
  );
}
