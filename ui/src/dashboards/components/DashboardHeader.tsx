// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  Button,
  IconFont,
  ButtonShape,
  ComponentColor,
} from '@influxdata/clockface'

// Constants
import {
  DEFAULT_DASHBOARD_NAME,
  DASHBOARD_NAME_MAX_LENGTH,
} from 'src/dashboards/constants/index'

// Actions
import {addNote} from 'src/dashboards/actions/v2/notes'

// Types
import * as AppActions from 'src/types/actions/app'
import * as QueriesModels from 'src/types/queries'
import {Dashboard} from '@influxdata/influx'

interface OwnProps {
  activeDashboard: string
  dashboard: Dashboard
  timeRange: QueriesModels.TimeRange
  autoRefresh: number
  handleChooseTimeRange: (timeRange: QueriesModels.TimeRange) => void
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  onManualRefresh: () => void
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onAddCell: () => void
  showTemplateControlBar: boolean
  zoomedTimeRange: QueriesModels.TimeRange
  onRenameDashboard: (name: string) => Promise<void>
  toggleVariableDropdowns: () => void
  isShowingVariableDropdowns: boolean
  isHidden: boolean
}

interface DispatchProps {
  onAddNote: typeof addNote
}

type Props = OwnProps & DispatchProps

class DashboardHeader extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    zoomedTimeRange: {
      upper: null,
      lower: null,
    },
  }

  public render() {
    const {
      handleChooseAutoRefresh,
      onManualRefresh,
      autoRefresh,
      handleChooseTimeRange,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
      isHidden,
      onAddNote,
      toggleVariableDropdowns,
      isShowingVariableDropdowns,
    } = this.props

    return (
      <Page.Header fullWidth={true} inPresentationMode={isHidden}>
        <Page.Header.Left>{this.dashboardTitle}</Page.Header.Left>
        <Page.Header.Right>
          <GraphTips />
          {this.addCellButton}
          <Button
            icon={IconFont.TextBlock}
            text="Add Note"
            onClick={onAddNote}
          />
          <AutoRefreshDropdown
            onChoose={handleChooseAutoRefresh}
            onManualRefresh={onManualRefresh}
            selected={autoRefresh}
          />
          <TimeRangeDropdown
            onSetTimeRange={handleChooseTimeRange}
            timeRange={{
              upper: zoomedUpper || upper,
              lower: zoomedLower || lower,
            }}
          />
          <Button
            text="Variables"
            onClick={toggleVariableDropdowns}
            color={
              isShowingVariableDropdowns
                ? ComponentColor.Primary
                : ComponentColor.Default
            }
          />
          <Button
            icon={IconFont.ExpandA}
            titleText="Enter Presentation Mode"
            shape={ButtonShape.Square}
            onClick={this.handleClickPresentationButton}
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private get addCellButton(): JSX.Element {
    const {dashboard, onAddCell} = this.props

    if (dashboard) {
      return (
        <Button
          icon={IconFont.AddCell}
          color={ComponentColor.Primary}
          onClick={onAddCell}
          text="Add Cell"
          titleText="Add cell to dashboard"
        />
      )
    }
  }

  private get dashboardTitle(): JSX.Element {
    const {dashboard, activeDashboard, onRenameDashboard} = this.props

    if (dashboard) {
      return (
        <RenamablePageTitle
          maxLength={DASHBOARD_NAME_MAX_LENGTH}
          onRename={onRenameDashboard}
          name={activeDashboard}
          placeholder={DEFAULT_DASHBOARD_NAME}
        />
      )
    }

    return <Page.Title title={activeDashboard} />
  }
}

const mdtp = {
  onAddNote: addNote,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(DashboardHeader)
