import { expect, test } from '@playwright/test'
import { writeAsOperator } from './consoleKit'

function documentRoute(node: string, doc: string): string {
  return `/console/repository/document?account=corpus-gamma&graph=intake:corpus-gamma&node=${node}&doc=${doc}`
}

test('a signed-in session without a writer credential is refused the delete by name', async ({ page }) => {
  await page.goto(documentRoute('ds-crawler', 'note-0001'))
  await page.getByTestId('delete-document').click()
  await page.getByTestId('delete-confirm').click()
  const dialog = page.getByTestId('delete-dialog')
  await expect(dialog.getByTestId('rpc-failure')).toContainText('PERMISSION_DENIED')
  await expect(page.getByTestId('revision-chip')).toHaveText('revision 1 (current)')
})

test('a delete names the call number and revision, records a tombstone and keeps the history', async ({ page }) => {
  await writeAsOperator(page)
  await page.goto(documentRoute('ds-crawler', 'note-0001'))
  await expect(page.getByTestId('revision-chip')).toHaveText('revision 1 (current)')
  await page.getByTestId('delete-document').click()
  const dialog = page.getByTestId('delete-dialog')
  await expect(dialog.getByTestId('delete-call-number')).toHaveText('corpus-gamma / intake:corpus-gamma / ds-crawler / note-0001')
  await expect(dialog.getByTestId('delete-revision')).toHaveText('1')
  const operation = (await dialog.getByTestId('delete-operation').innerText()).trim()
  await dialog.getByTestId('delete-reason').locator('textarea').fill('removed from the browser leg')
  await dialog.getByTestId('delete-confirm').click()

  await expect(page.getByTestId('deleted-notice')).toBeVisible()
  await expect(page.getByTestId('revision-chip')).toContainText('revision 2')
  await page.getByTestId('tab-history').click()
  const tombstone = page.getByTestId('history').getByTestId('revision-2')
  await expect(tombstone).toContainText('tombstone')
  await expect(tombstone).toContainText(operation)
  // The bytes stay: revision 1 still opens with its parts.
  await page.getByTestId('history').getByTestId('open-revision-1').click()
  await page.getByRole('tab', { name: 'Parts' }).click()
  await expect(page.getByTestId('parts-table').getByText('PRESENT').first()).toBeVisible()
})

test('a delete against a revision that moved on is shown stale and never retried', async ({ page, context }) => {
  await writeAsOperator(page)
  await page.goto(documentRoute('ds-share', 'policy-travel'))
  const pinned = (await page.getByTestId('revision-chip').innerText()).match(/revision (\d+)/)?.[1]
  expect(pinned).toBeDefined()
  await page.getByTestId('delete-document').click()
  await expect(page.getByTestId('delete-revision')).toHaveText(pinned!)

  // Another tab deletes first.
  const other = await context.newPage()
  await writeAsOperator(other)
  await other.goto(documentRoute('ds-share', 'policy-travel'))
  await other.getByTestId('delete-document').click()
  await other.getByTestId('delete-confirm').click()
  await expect(other.getByTestId('deleted-notice')).toBeVisible()
  await other.close()

  await page.getByTestId('delete-confirm').click()
  const stale = page.getByTestId('delete-stale')
  await expect(stale).toContainText(String(Number(pinned) + 1))
  await expect(page.getByTestId('delete-confirm')).toBeDisabled()
  // Reloading opens the tombstone the other tab recorded, not an error for the deleted current.
  await stale.getByTestId('delete-reload').click()
  await expect(page.getByTestId('revision-chip')).toContainText(`revision ${Number(pinned) + 1}`)
  await expect(page.getByTestId('rpc-failure')).toHaveCount(0)
  await page.getByTestId('tab-history').click()
  await expect(page.getByTestId('history').getByTestId(`revision-${Number(pinned) + 1}`)).toContainText('tombstone')
})
