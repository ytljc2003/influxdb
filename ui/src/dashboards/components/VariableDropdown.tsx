// Libraries
import React, {PureComponent} from 'react'
import {Dropdown} from 'src/clockface'

// Types
import {VariableValuesByID} from 'src/variables/types'

interface Props {
  name: string
  values: any //todo
  initialSelected: any // todo
}

interface State {
  selectedValue: any // todo
}

class VariableDropdown extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    const {values, initialSelected} = this.props
    console.log('this props values: ', values)
    console.log('initial: ', initialSelected)
    this.state = {
      selectedValue: initialSelected,
    }
  }

  render() {
    return (
      <Dropdown selectedID={this.state.selectedValue}>
        {this.dropdownItems}
      </Dropdown>
    )
  }

  private get dropdownItems(): JSX.Element[] {
    const {values} = this.props
    console.log(values)

    return Object.keys(values).map(v => {
      return <Dropdown.Item key={v}>{v}</Dropdown.Item>
    })
  }
}

export default VariableDropdown
