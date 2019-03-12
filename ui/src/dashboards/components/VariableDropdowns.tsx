// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import VariableDropdown from 'src/dashboards/components/VariableDropdown'

// Types
import {AppState} from 'src/types/v2'
import {VariablesState} from 'src/variables/reducers'

interface OwnProps {
  dashboardID: string
}

interface StateProps {
  variables: VariablesState
}

class VariableDropdowns extends PureComponent<StateProps & OwnProps> {
  render() {
    const {variables, values} = this.props.variables
    const {dashboardID} = this.props

    if (variables) {
      console.log(variables)
    }

    return (
      <div style={{display: 'flex'}}>
        {Object.keys(variables).map(variableID => {
          const initialSelected = _.get(
            values,
            `${dashboardID}.values[${variableID}].selectedValue`
          )
          const valuesArray = _.get(
            values,
            `${dashboardID}.values[${variableID}].values`
          )

          console.log(valuesArray)

          return (
            <VariableDropdown
              key={variableID}
              name={variables[variableID].variable.name}
              values={valuesArray}
              initialSelected={initialSelected}
            />
          )
        })}
      </div>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {variables} = state

  return {variables}
}

export default connect(mstp)(VariableDropdowns)
